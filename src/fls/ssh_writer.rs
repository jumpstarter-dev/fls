use std::io::{self, BufReader, Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use tokio::sync::mpsc;

use super::block_writer::WriterCommand;
use super::options::FlashOptions;

const MAGIC: [u8; 4] = *b"FLSW";
const PROTO_VER: u8 = 1;
const OP_DATA: u8 = 0x01;
const OP_SEEK: u8 = 0x03;
const OP_SYNC: u8 = 0x04;
const OP_QUIT: u8 = 0x05;
const OP_ZERO: u8 = 0x09;
const OP_SIZE: u8 = 0x0a;
const R_READY: u8 = 0x81;
const R_ACK: u8 = 0x82;
const R_PROG: u8 = 0x83;
const R_ERR: u8 = 0x84;
const R_DONE: u8 = 0x85;
const R_SIZE: u8 = 0x89;
const CHUNK_SIZE: usize = 8 << 20;
const EMBEDDED_WH: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/flswh-aarch64-qnx7"));
const REMOTE_WH: &str = "/tmp/fls-wh";

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn encode_frame(op: u8, payload: &[u8]) -> io::Result<Vec<u8>> {
    let length = u32::try_from(payload.len()).map_err(|_| invalid("Frame payload too large"))?;
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.extend_from_slice(&MAGIC);
    frame.push(op);
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn encode_data_frame(data: &[u8]) -> io::Result<Vec<u8>> {
    if data.len() > CHUNK_SIZE {
        return Err(invalid("DATA chunk exceeds 8 MiB"));
    }
    let mut payload = Vec::with_capacity(data.len() + 8);
    payload.extend_from_slice(&(data.len() as u32).to_le_bytes());
    payload.extend_from_slice(data);
    payload.extend_from_slice(&crc32fast::hash(data).to_le_bytes());
    encode_frame(OP_DATA, &payload)
}

struct FrameReader<R: Read> {
    reader: BufReader<R>,
}

impl<R: Read> FrameReader<R> {
    fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }

    fn read_frame(&mut self) -> io::Result<(u8, Vec<u8>)> {
        let mut magic = [0; 4];
        self.reader.read_exact(&mut magic)?;
        while magic != MAGIC {
            magic.copy_within(1.., 0);
            self.reader.read_exact(&mut magic[3..])?;
        }
        let mut header = [0; 5];
        self.reader.read_exact(&mut header)?;
        let length = u32::from_le_bytes(header[1..].try_into().unwrap()) as usize;
        // READ responses are capped at 1 MiB; all other records are smaller.
        if length > 1 << 20 {
            return Err(invalid("Remote response exceeds 1 MiB"));
        }
        let mut payload = vec![0; length];
        self.reader.read_exact(&mut payload)?;
        Ok((header[0], payload))
    }
}

pub(crate) fn parse_ssh_target(device: &str) -> Option<(String, String)> {
    if device.starts_with('/') {
        return None;
    }
    device
        .split_once(':')
        .map(|(host, path)| (host.to_owned(), path.to_owned()))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn ssh_command(host: &str, remote_command: &str, options: &FlashOptions) -> Command {
    let mut command = if let Some(file) = &options.ssh_password_file {
        let mut command = Command::new("sshpass");
        command.args(["-f", file, "ssh"]);
        command
    } else if std::env::var_os("SSHPASS").is_some() {
        let mut command = Command::new("sshpass");
        command.args(["-e", "ssh"]);
        command
    } else {
        Command::new("ssh")
    };
    if options.ssh_compress {
        command.arg("-C");
    }
    command.args(["-T", "--", host, remote_command]);
    command
}

fn upload_wh(host: &str, binary: &[u8], options: &FlashOptions) -> io::Result<()> {
    // QNX boot-image utilities may be absent from the default PATH.
    let remote_command =
        format!("PATH=/proc/boot:/usr/bin:/bin:$PATH; cat > {REMOTE_WH} && chmod +x {REMOTE_WH}");
    // ponytail: one upload path per host; use unique paths for concurrent flash sessions.
    let mut child = ssh_command(host, &remote_command, options)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    let result = child.stdin.take().unwrap().write_all(binary);
    if let Err(error) = result {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    let status = child.wait()?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "Write-head upload failed: {status}"
        )));
    }
    Ok(())
}

struct Session {
    child: Child,
    input: Option<ChildStdin>,
    output: FrameReader<ChildStdout>,
    progress: mpsc::UnboundedSender<u64>,
    cursor: u64,
    size: u64,
}

impl Session {
    fn new(mut child: Child, progress: mpsc::UnboundedSender<u64>) -> io::Result<Self> {
        let input = child.stdin.take().unwrap();
        let output = FrameReader::new(child.stdout.take().unwrap());
        let mut session = Self {
            child,
            input: Some(input),
            output,
            progress,
            cursor: 0,
            size: 0,
        };
        let (op, payload) = session.response()?;
        if op != R_READY || payload.len() != 9 || payload[0] != PROTO_VER {
            return Err(invalid(
                "Invalid write-head READY or unsupported protocol version",
            ));
        }
        let advertised = u64::from_le_bytes(payload[1..].try_into().unwrap());
        // SIZE distinguishes an unknown startup size from a genuinely empty device.
        session.send(OP_SIZE, &[])?;
        let (op, payload) = session.response()?;
        if op != R_SIZE || payload.len() != 8 {
            return Err(invalid("Invalid write-head SIZE response"));
        }
        session.size = u64::from_le_bytes(payload[..].try_into().unwrap());
        if advertised != session.size {
            return Err(invalid("READY and SIZE disagree about device capacity"));
        }
        Ok(session)
    }

    fn send(&mut self, op: u8, payload: &[u8]) -> io::Result<()> {
        self.input
            .as_mut()
            .unwrap()
            .write_all(&encode_frame(op, payload)?)
    }

    fn response(&mut self) -> io::Result<(u8, Vec<u8>)> {
        let (op, payload) = self.output.read_frame()?;
        if op == R_ERR {
            if payload.len() != 14 {
                return Err(invalid("Malformed remote ERR"));
            }
            let code = payload[0];
            let command = payload[1];
            let errno = u32::from_le_bytes(payload[2..6].try_into().unwrap());
            let offset = u64::from_le_bytes(payload[6..].try_into().unwrap());
            let reason = match code {
                0 => "device I/O error",
                1 => "CRC mismatch",
                2 => "unsupported or malformed command",
                _ => "unknown error",
            };
            return Err(io::Error::other(format!(
                "Remote {reason}: opcode {command:#04x}, errno {errno}, offset {offset}"
            )));
        }
        Ok((op, payload))
    }

    fn expect_ok(&mut self, command: u8, start: u64, bytes: u64) -> io::Result<()> {
        let mut last_done = 0;
        loop {
            let (op, payload) = self.response()?;
            if op == R_PROG && matches!(command, OP_DATA | OP_ZERO) && payload.len() == 16 {
                let offset = u64::from_le_bytes(payload[..8].try_into().unwrap());
                let done = u64::from_le_bytes(payload[8..].try_into().unwrap());
                if done < last_done || done > bytes || start.checked_add(done) != Some(offset) {
                    return Err(invalid("Invalid remote PROG range"));
                }
                last_done = done;
                let _ = self.progress.send(offset);
            } else if op == R_ACK && payload.len() == 17 && payload[0] == command {
                let offset = u64::from_le_bytes(payload[1..9].try_into().unwrap());
                let written = u64::from_le_bytes(payload[9..].try_into().unwrap());
                if offset != start || written != bytes {
                    return Err(invalid("Remote OK does not match the command range"));
                }
                return Ok(());
            } else {
                return Err(invalid(format!(
                    "Unexpected response {op:#04x} to {command:#04x}"
                )));
            }
        }
    }

    fn check_range(&self, bytes: u64) -> io::Result<()> {
        if self.cursor > self.size || bytes > self.size - self.cursor {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Write at {} of {bytes} bytes exceeds remote device size {}",
                    self.cursor, self.size
                ),
            ));
        }
        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.check_range(data.len() as u64)?;
        for chunk in data.chunks(CHUNK_SIZE) {
            self.input
                .as_mut()
                .unwrap()
                .write_all(&encode_data_frame(chunk)?)?;
            self.expect_ok(OP_DATA, self.cursor, chunk.len() as u64)?;
            self.cursor += chunk.len() as u64;
            let _ = self.progress.send(self.cursor);
        }
        Ok(())
    }

    fn seek(&mut self, offset: u64) -> io::Result<()> {
        if offset > self.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Seek exceeds remote device size",
            ));
        }
        self.send(OP_SEEK, &offset.to_le_bytes())?;
        self.expect_ok(OP_SEEK, offset, 0)?;
        self.cursor = offset;
        let _ = self.progress.send(offset);
        Ok(())
    }

    fn fill(&mut self, pattern: [u8; 4], bytes: u64) -> io::Result<()> {
        self.check_range(bytes)?;
        if pattern == [0; 4] {
            self.send(OP_ZERO, &bytes.to_le_bytes())?;
            self.expect_ok(OP_ZERO, self.cursor, bytes)?;
            self.cursor += bytes;
            let _ = self.progress.send(self.cursor);
        } else {
            // ponytail: nonzero fills travel as DATA; add FILL if bandwidth matters.
            let mut buffer = vec![0; CHUNK_SIZE];
            for chunk in buffer.chunks_exact_mut(4) {
                chunk.copy_from_slice(&pattern);
            }
            let mut remaining = bytes;
            while remaining > 0 {
                let n = remaining.min(CHUNK_SIZE as u64) as usize;
                self.write(&buffer[..n])?;
                remaining -= n as u64;
            }
        }
        Ok(())
    }

    fn finish(mut self) -> io::Result<u64> {
        self.send(OP_SYNC, &[])?;
        self.expect_ok(OP_SYNC, self.cursor, 0)?;
        self.send(OP_QUIT, &[])?;
        let (op, payload) = self.response()?;
        if op != R_DONE || payload.len() != 8 {
            return Err(invalid("Invalid write-head DONE"));
        }
        self.input.take();
        let status = self.child.wait()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "Remote writer exited with {status}"
            )));
        }
        Ok(u64::from_le_bytes(payload[..].try_into().unwrap()))
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.input.take();
        // std::process::Child does not reap or terminate on drop.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(crate) struct SshBlockWriter {
    writer_tx: mpsc::Sender<WriterCommand>,
    writer_handle: tokio::task::JoinHandle<io::Result<u64>>,
}

impl SshBlockWriter {
    pub(crate) fn new(
        host: String,
        device: String,
        progress: mpsc::UnboundedSender<u64>,
        options: &FlashOptions,
    ) -> io::Result<Self> {
        if host.is_empty()
            || host.starts_with('-')
            || host.contains('\0')
            || device.is_empty()
            || !device.starts_with('/')
            || device.contains('\0')
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Expected [user@]host:/absolute/device",
            ));
        }
        let binary = match &options.wh_bin {
            Some(path) => std::fs::read(path)?,
            None => EMBEDDED_WH.to_vec(),
        };
        if binary.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "No embedded write head: build it with make remote-wh or pass --wh-bin",
            ));
        }
        let options = options.clone();
        let (writer_tx, mut writer_rx) = mpsc::channel((options.write_buffer_size_mb / 8).max(1));
        let writer_handle = tokio::task::spawn_blocking(move || {
            upload_wh(&host, &binary, &options)?;
            let remote_command = format!("{REMOTE_WH} {}", shell_quote(&device));
            let child = ssh_command(&host, &remote_command, &options)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?;
            let mut session = Session::new(child, progress)?;
            if options.debug {
                eprintln!("[DEBUG] Remote device size: {} bytes", session.size);
            }
            while let Some(command) = writer_rx.blocking_recv() {
                match command {
                    WriterCommand::Write(data) => session.write(&data)?,
                    WriterCommand::Seek(offset) => session.seek(offset)?,
                    WriterCommand::Fill { pattern, bytes } => session.fill(pattern, bytes)?,
                }
            }
            session.finish()
        });
        Ok(Self {
            writer_tx,
            writer_handle,
        })
    }

    pub(crate) async fn write(&self, data: Vec<u8>) -> io::Result<()> {
        self.send(WriterCommand::Write(data)).await
    }

    pub(crate) async fn seek(&self, offset: u64) -> io::Result<()> {
        self.send(WriterCommand::Seek(offset)).await
    }

    pub(crate) async fn fill(&self, pattern: [u8; 4], bytes: u64) -> io::Result<()> {
        self.send(WriterCommand::Fill { pattern, bytes }).await
    }

    async fn send(&self, command: WriterCommand) -> io::Result<()> {
        self.writer_tx
            .send(command)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "SSH writer channel closed"))
    }

    pub(crate) async fn close(self) -> io::Result<u64> {
        drop(self.writer_tx);
        self.writer_handle.await.map_err(io::Error::other)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek};
    use std::sync::OnceLock;

    fn native_binary() -> &'static std::path::Path {
        static BUILD: OnceLock<tempfile::TempDir> = OnceLock::new();
        BUILD
            .get_or_init(|| {
                let dir = tempfile::tempdir().unwrap();
                let source =
                    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("remote/fls-wh.c");
                assert!(Command::new("cc")
                    .arg("-O2")
                    .arg(source)
                    .arg("-o")
                    .arg(dir.path().join("fls-wh"))
                    .status()
                    .unwrap()
                    .success());
                dir
            })
            .path()
    }

    fn native_session(device: &std::path::Path) -> Session {
        let child = Command::new(native_binary().join("fls-wh"))
            .arg(device)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        Session::new(child, mpsc::unbounded_channel().0).unwrap()
    }

    #[test]
    fn framing_crc_resync_and_bounds() {
        let encoded = encode_data_frame(b"123456789").unwrap();
        assert_eq!(&encoded[..13], b"FLSW\x01\x11\x00\x00\x00\x09\x00\x00\x00");
        assert_eq!(&encoded[encoded.len() - 4..], &0xcbf43926u32.to_le_bytes());
        let mut stream = b"junkFLSF".to_vec();
        stream.extend_from_slice(&encoded);
        let (op, payload) = FrameReader::new(Cursor::new(stream)).read_frame().unwrap();
        assert_eq!(op, OP_DATA);
        assert_eq!(payload.len(), 17);
        let mut truncated = encoded.clone();
        truncated.pop();
        assert_eq!(
            FrameReader::new(Cursor::new(truncated))
                .read_frame()
                .unwrap_err()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
        let mut oversized = b"FLSW\x86".to_vec();
        oversized.extend_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            FrameReader::new(Cursor::new(oversized))
                .read_frame()
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn target_parsing_auth_and_shell_quoting() {
        assert_eq!(
            parse_ssh_target("root@board:/dev/emmc0"),
            Some(("root@board".into(), "/dev/emmc0".into()))
        );
        assert_eq!(parse_ssh_target("/dev/disk:local"), None);
        assert_eq!(parse_ssh_target("image.img"), None);
        let options = FlashOptions {
            ssh_password_file: Some("password file".into()),
            ssh_compress: true,
            ..Default::default()
        };
        let command = ssh_command("board", "echo ready", &options);
        assert_eq!(command.get_program(), "sshpass");
        let args: Vec<_> = command
            .get_args()
            .map(|arg| arg.to_str().unwrap())
            .collect();
        assert_eq!(
            args,
            [
                "-f",
                "password file",
                "ssh",
                "-C",
                "-T",
                "--",
                "board",
                "echo ready"
            ]
        );
        let path = "/dev/a'b; $(echo injected)";
        let output = Command::new("sh")
            .args(["-c", &format!("printf '%s' {}", shell_quote(path))])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, path.as_bytes());
    }

    #[test]
    fn native_write_head_round_trip_and_capacity() {
        let mut device = tempfile::NamedTempFile::new().unwrap();
        device.write_all(&vec![0x55; CHUNK_SIZE * 3]).unwrap();
        let mut session = native_session(device.path());
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        session.progress = progress_tx;
        session.write(&vec![0xa5; CHUNK_SIZE + 17]).unwrap();
        session.seek(32).unwrap();
        session.fill([0; 4], (CHUNK_SIZE + 5) as u64).unwrap();
        session.seek(16).unwrap();
        session.fill([1, 2, 3, 4], 7).unwrap();
        assert_eq!(
            session.fill([0; 4], u64::MAX).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            session.seek(u64::MAX).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        session.seek(session.size - 1).unwrap();
        assert_eq!(
            session.write(&[1, 2]).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let total = session.finish().unwrap();
        assert_eq!(total, (2 * CHUNK_SIZE + 29) as u64);
        assert!(progress_rx.try_recv().is_ok());
        device.seek(io::SeekFrom::Start(0)).unwrap();
        let mut prefix = [0; 32];
        device.read_exact(&mut prefix).unwrap();
        assert_eq!(&prefix[..16], &[0xa5; 16]);
        assert_eq!(&prefix[16..23], &[1, 2, 3, 4, 1, 2, 3]);
        let mut zeros = vec![1; CHUNK_SIZE + 5];
        device.read_exact(&mut zeros).unwrap();
        assert!(zeros.iter().all(|&byte| byte == 0));
        let mut tail = [0; 1];
        device.read_exact(&mut tail).unwrap();
        assert_eq!(tail, [0x55]);
    }

    #[test]
    fn native_crc_error_is_propagated() {
        let device = tempfile::NamedTempFile::new().unwrap();
        device.as_file().set_len(32).unwrap();
        let mut session = native_session(device.path());
        let mut frame = encode_data_frame(b"test").unwrap();
        *frame.last_mut().unwrap() ^= 1;
        session.input.as_mut().unwrap().write_all(&frame).unwrap();
        let error = session.expect_ok(OP_DATA, 0, 4).unwrap_err();
        assert!(error.to_string().contains("CRC mismatch"), "{error}");
        assert!(error.to_string().contains("opcode 0x01"), "{error}");
    }
}
