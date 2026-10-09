use std::io::{self, BufReader, Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{mpsc as std_mpsc, Arc, Condvar, Mutex};
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
    if let Some(port) = options.ssh_port {
        command.arg("-p").arg(port.to_string());
    }
    // Streaming stdin belongs to the protocol; terminal prompts would stall it.
    if options.strict_ssh_host_key_checking {
        command.args(["-o", "StrictHostKeyChecking=yes"]);
    } else {
        command.args([
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
        ]);
    }
    if command.get_program() == "ssh" {
        command.args(["-o", "BatchMode=yes"]);
    }
    command.args(["-T", "--", host, remote_command]);
    command
}

fn detect_target(host: &str, options: &FlashOptions) -> io::Result<(String, String, String)> {
    let command = "PATH=/proc/boot:/usr/bin:/bin:$PATH; \
        os=$(uname -s) || exit; echo \"$os\"; \
        if [ \"$os\" = QNX ]; then uname -p; else uname -m; fi || exit; uname -r";
    let output = ssh_command(host, command, options)
        .stdin(Stdio::null())
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let reason = if stderr.trim().is_empty() {
            "remote platform probe failed without diagnostic output"
        } else {
            stderr.trim()
        };
        let port = options
            .ssh_port
            .map(|port| format!(" -p {port}"))
            .unwrap_or_default();
        return Err(io::Error::other(format!(
            "SSH to {host} failed ({}): {reason}\nCheck the connection with `ssh{port} {}`",
            output.status,
            shell_quote(host)
        )));
    }
    let text = std::str::from_utf8(&output.stdout)
        .map_err(|_| invalid("Remote platform response is not UTF-8"))?;
    let fields: Vec<_> = text.lines().map(str::trim).collect();
    if fields.len() != 3 || fields.iter().any(|field| field.is_empty()) {
        return Err(invalid(
            "Expected remote OS, processor architecture, and release",
        ));
    }
    Ok((fields[0].into(), fields[1].into(), fields[2].into()))
}

fn check_embedded_target(os: &str, arch: &str, release: &str) -> io::Result<()> {
    if os == "QNX"
        && matches!(arch, "aarch64le" | "aarch64" | "arm64")
        && release.split('.').next() == Some("7")
    {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("Embedded write head requires aarch64 QNX 7; remote is {os} {arch} {release}. Pass --wh-bin built for this target"),
    ))
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

fn read_response<R: Read>(output: &mut FrameReader<R>) -> io::Result<(u8, Vec<u8>)> {
    let (op, payload) = output.read_frame()?;
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

struct Expected {
    op: u8,
    start: u64,
    bytes: u64,
    charge: usize,
}

#[derive(Default)]
struct FlowState {
    bytes: usize,
    commands: usize,
    error: Option<io::Error>,
}

struct FlowControl {
    state: Mutex<FlowState>,
    ready: Condvar,
    limit: usize,
}

impl FlowControl {
    fn new(limit: usize) -> Self {
        Self {
            state: Mutex::new(FlowState::default()),
            ready: Condvar::new(),
            limit,
        }
    }

    fn reserve(&self, bytes: usize) -> io::Result<()> {
        if bytes > self.limit {
            return Err(invalid("Frame exceeds SSH in-flight window"));
        }
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(error) = &state.error {
                return Err(io::Error::new(error.kind(), error.to_string()));
            }
            // Also bound metadata for tiny ZERO/SEEK requests.
            if state.bytes <= self.limit - bytes && state.commands < 1024 {
                state.bytes += bytes;
                state.commands += 1;
                return Ok(());
            }
            state = self.ready.wait(state).unwrap();
        }
    }

    fn complete(&self, bytes: usize) {
        let mut state = self.state.lock().unwrap();
        state.bytes -= bytes;
        state.commands -= 1;
        self.ready.notify_all();
    }

    fn fail(&self, error: &io::Error) {
        let mut state = self.state.lock().unwrap();
        if state.error.is_none() {
            state.error = Some(io::Error::new(error.kind(), error.to_string()));
        }
        self.ready.notify_all();
    }

    fn wait_idle(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(error) = &state.error {
                return Err(io::Error::new(error.kind(), error.to_string()));
            }
            if state.commands == 0 {
                return Ok(());
            }
            state = self.ready.wait(state).unwrap();
        }
    }
}

fn expect_ok<R: Read>(
    output: &mut FrameReader<R>,
    expected: &Expected,
    progress: &mpsc::UnboundedSender<u64>,
) -> io::Result<()> {
    let mut last_done = 0;
    loop {
        let (op, payload) = read_response(output)?;
        if op == R_PROG && matches!(expected.op, OP_DATA | OP_ZERO) && payload.len() == 16 {
            let offset = u64::from_le_bytes(payload[..8].try_into().unwrap());
            let done = u64::from_le_bytes(payload[8..].try_into().unwrap());
            if done < last_done
                || done > expected.bytes
                || expected.start.checked_add(done) != Some(offset)
            {
                return Err(invalid("Invalid remote PROG range"));
            }
            last_done = done;
            let _ = progress.send(offset);
        } else if op == R_ACK && payload.len() == 17 && payload[0] == expected.op {
            let offset = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let written = u64::from_le_bytes(payload[9..].try_into().unwrap());
            if offset != expected.start || written != expected.bytes {
                return Err(invalid("Remote OK does not match the command range"));
            }
            if matches!(expected.op, OP_DATA | OP_ZERO | OP_SEEK) {
                let _ = progress.send(expected.start + expected.bytes);
            }
            return Ok(());
        } else {
            return Err(invalid(format!(
                "Unexpected response {op:#04x} to {:#04x}",
                expected.op
            )));
        }
    }
}

fn read_acks(
    mut output: FrameReader<ChildStdout>,
    requests: std_mpsc::Receiver<Expected>,
    flow: Arc<FlowControl>,
    child: Arc<Mutex<Child>>,
    progress: mpsc::UnboundedSender<u64>,
) -> io::Result<u64> {
    let result = (|| {
        let mut total = 0u64;
        while let Ok(expected) = requests.recv() {
            if expected.op == OP_QUIT {
                let (op, payload) = read_response(&mut output)?;
                if op != R_DONE || payload.len() != 8 {
                    return Err(invalid("Invalid write-head DONE"));
                }
                if u64::from_le_bytes(payload[..].try_into().unwrap()) != total {
                    return Err(invalid("DONE total does not match acknowledged writes"));
                }
                flow.complete(expected.charge);
                return Ok(total);
            }
            expect_ok(&mut output, &expected, &progress)?;
            if matches!(expected.op, OP_DATA | OP_ZERO) {
                total = total
                    .checked_add(expected.bytes)
                    .ok_or_else(|| invalid("Written byte count overflow"))?;
            }
            flow.complete(expected.charge);
        }
        Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "SSH writer closed without QUIT",
        ))
    })();
    if let Err(ref error) = result {
        flow.fail(error);
        // Interrupt a sender blocked in write_all when the peer reports an error.
        let _ = child.lock().unwrap().kill();
    }
    result
}

struct Session {
    child: Arc<Mutex<Child>>,
    input: Option<ChildStdin>,
    requests: Option<std_mpsc::Sender<Expected>>,
    reader: Option<std::thread::JoinHandle<io::Result<u64>>>,
    flow: Arc<FlowControl>,
    cursor: u64,
    size: u64,
    pending: Vec<u8>,
    frame_size: usize,
}

impl Session {
    fn new(
        mut child: Child,
        progress: mpsc::UnboundedSender<u64>,
        window: usize,
    ) -> io::Result<Self> {
        let input = child.stdin.take().unwrap();
        let mut output = FrameReader::new(child.stdout.take().unwrap());
        let (tx, rx) = std_mpsc::channel();
        let mut session = Self {
            child: Arc::new(Mutex::new(child)),
            input: Some(input),
            requests: Some(tx),
            reader: None,
            flow: Arc::new(FlowControl::new(window)),
            cursor: 0,
            size: 0,
            pending: Vec::with_capacity(window.min(CHUNK_SIZE)),
            frame_size: window.min(CHUNK_SIZE),
        };
        let (op, payload) = read_response(&mut output)?;
        if op != R_READY || payload.len() != 9 || payload[0] != PROTO_VER {
            return Err(invalid(
                "Invalid write-head READY or unsupported protocol version",
            ));
        }
        let advertised = u64::from_le_bytes(payload[1..].try_into().unwrap());
        // SIZE distinguishes an unknown startup size from a genuinely empty device.
        session
            .input
            .as_mut()
            .unwrap()
            .write_all(&encode_frame(OP_SIZE, &[])?)?;
        let (op, payload) = read_response(&mut output)?;
        if op != R_SIZE || payload.len() != 8 {
            return Err(invalid("Invalid write-head SIZE response"));
        }
        session.size = u64::from_le_bytes(payload[..].try_into().unwrap());
        if advertised != session.size {
            return Err(invalid("READY and SIZE disagree about device capacity"));
        }
        let flow = Arc::clone(&session.flow);
        let child = Arc::clone(&session.child);
        session.reader = Some(
            std::thread::Builder::new()
                .name("fls-ssh-acks".into())
                .spawn(move || read_acks(output, rx, flow, child, progress))?,
        );
        Ok(session)
    }

    fn queue_frame(&mut self, op: u8, start: u64, bytes: u64, frame: &[u8]) -> io::Result<()> {
        let charge = if op == OP_DATA { bytes as usize } else { 1 };
        self.flow.reserve(charge)?;
        self.requests
            .as_ref()
            .unwrap()
            .send(Expected {
                op,
                start,
                bytes,
                charge,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "SSH response reader closed"))?;
        self.input
            .as_mut()
            .unwrap()
            .write_all(frame)
            .map_err(|error| {
                self.flow.fail(&error);
                let state = self.flow.state.lock().unwrap();
                let error = state.error.as_ref().unwrap();
                io::Error::new(error.kind(), error.to_string())
            })
    }

    fn send(&mut self, op: u8, start: u64, bytes: u64, payload: &[u8]) -> io::Result<()> {
        self.queue_frame(op, start, bytes, &encode_frame(op, payload)?)
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
        let mut remaining = data;
        while !remaining.is_empty() {
            let n = remaining.len().min(self.frame_size - self.pending.len());
            self.pending.extend_from_slice(&remaining[..n]);
            self.cursor += n as u64;
            remaining = &remaining[n..];
            if self.pending.len() == self.frame_size {
                self.flush_pending()?;
            }
        }
        Ok(())
    }

    fn flush_pending(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let bytes = self.pending.len() as u64;
        let start = self.cursor - bytes;
        self.queue_frame(OP_DATA, start, bytes, &encode_data_frame(&self.pending)?)?;
        self.pending.clear();
        Ok(())
    }

    fn seek(&mut self, offset: u64) -> io::Result<()> {
        if offset > self.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Seek exceeds remote device size",
            ));
        }
        self.flush_pending()?;
        self.send(OP_SEEK, offset, 0, &offset.to_le_bytes())?;
        self.cursor = offset;
        Ok(())
    }

    fn fill(&mut self, pattern: [u8; 4], bytes: u64) -> io::Result<()> {
        self.check_range(bytes)?;
        self.flush_pending()?;
        if pattern == [0; 4] {
            self.send(OP_ZERO, self.cursor, bytes, &bytes.to_le_bytes())?;
            self.cursor += bytes;
        } else {
            // ponytail: nonzero fills travel as DATA; add FILL if bandwidth matters.
            let mut buffer = vec![0; CHUNK_SIZE];
            buffer.as_chunks_mut::<4>().0.fill(pattern);
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
        self.flush_pending()?;
        self.flow.wait_idle()?;
        self.send(OP_SYNC, self.cursor, 0, &[])?;
        self.send(OP_QUIT, self.cursor, 0, &[])?;
        self.input.take();
        self.requests.take();
        self.reader
            .take()
            .unwrap()
            .join()
            .map_err(|_| io::Error::other("SSH response reader panicked"))??;
        let status = self.child.lock().unwrap().wait()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "Remote writer exited with {status}"
            )));
        }
        // Match the local writer: completion includes sparse seeks/skips.
        // The ACK reader separately validates DONE's physical write count.
        Ok(self.cursor)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.input.take();
        self.requests.take();
        // std::process::Child does not reap or terminate on drop.
        let _ = self.child.lock().unwrap().kill();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        let _ = self.child.lock().unwrap().wait();
    }
}

pub(crate) struct SshBlockWriter {
    writer_tx: mpsc::Sender<WriterCommand>,
    writer_handle: tokio::task::JoinHandle<io::Result<u64>>,
    writer_error: Arc<Mutex<Option<io::Error>>>,
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
        if options.ssh_port == Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SSH port must be between 1 and 65535",
            ));
        }
        let window = options
            .write_buffer_size_mb
            .max(1)
            .checked_mul(1 << 20)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SSH write buffer size overflow",
                )
            })?;
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
        let writer_error = Arc::new(Mutex::new(None));
        let error_state = Arc::clone(&writer_error);
        let writer_handle = tokio::task::spawn_blocking(move || {
            let result = (|| {
                let (os, arch, release) = detect_target(&host, &options)?;
                if options.wh_bin.is_none() {
                    check_embedded_target(&os, &arch, &release)?;
                }
                if options.debug {
                    eprintln!("[DEBUG] Remote platform: {os} {arch} {release}");
                }
                upload_wh(&host, &binary, &options)?;
                let remote_command = format!("{REMOTE_WH} {}", shell_quote(&device));
                let child = ssh_command(&host, &remote_command, &options)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()?;
                let mut session = Session::new(child, progress, window)?;
                if options.debug {
                    eprintln!("[DEBUG] Remote device size: {} bytes", session.size);
                    eprintln!("[DEBUG] SSH in-flight DATA window: {} MiB", window >> 20);
                }
                while let Some(command) = writer_rx.blocking_recv() {
                    match command {
                        WriterCommand::Write(data) => session.write(&data)?,
                        WriterCommand::Seek(offset) => session.seek(offset)?,
                        WriterCommand::Fill { pattern, bytes } => session.fill(pattern, bytes)?,
                    }
                }
                session.finish()
            })();
            if let Err(ref error) = result {
                *error_state.lock().unwrap() =
                    Some(io::Error::new(error.kind(), error.to_string()));
            }
            result
        });
        Ok(Self {
            writer_tx,
            writer_handle,
            writer_error,
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
        self.writer_tx.send(command).await.map_err(|_| {
            match self.writer_error.lock().unwrap().as_ref() {
                Some(error) => io::Error::new(error.kind(), error.to_string()),
                None => io::Error::new(io::ErrorKind::BrokenPipe, "SSH writer channel closed"),
            }
        })
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

    fn native_child(device: &std::path::Path) -> Child {
        Command::new(native_binary().join("fls-wh"))
            .arg(device)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn native_session(device: &std::path::Path) -> Session {
        Session::new(
            native_child(device),
            mpsc::unbounded_channel().0,
            2 * CHUNK_SIZE,
        )
        .unwrap()
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
            ssh_port: Some(11223),
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
                "-p",
                "11223",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
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
        assert!(check_embedded_target("QNX", "aarch64le", "7.1.0").is_ok());
        for (os, arch, release) in [
            ("Linux", "aarch64", "6.1.0"),
            ("Darwin", "arm64", "25.0.0"),
            ("QNX", "x86_64", "7.1.0"),
            ("QNX", "aarch64le", "8.0.0"),
        ] {
            assert_eq!(
                check_embedded_target(os, arch, release).unwrap_err().kind(),
                io::ErrorKind::Unsupported
            );
        }
    }

    #[test]
    fn native_write_head_round_trip_and_capacity() {
        let mut device = tempfile::NamedTempFile::new().unwrap();
        device.write_all(&vec![0x55; CHUNK_SIZE * 3]).unwrap();
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let mut session =
            Session::new(native_child(device.path()), progress_tx, 2 * CHUNK_SIZE).unwrap();
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
        let final_offset = session.cursor;
        assert_eq!(session.finish().unwrap(), final_offset);
        assert_eq!(final_offset, (3 * CHUNK_SIZE - 1) as u64);
        let mut last_progress = None;
        while let Ok(offset) = progress_rx.try_recv() {
            last_progress = Some(offset);
        }
        assert_eq!(last_progress, Some(final_offset));
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
    fn small_writes_coalesce_and_flush_at_command_boundaries() {
        let mut device = tempfile::NamedTempFile::new().unwrap();
        device.as_file().set_len(16).unwrap();
        let mut session = native_session(device.path());
        session.write(b"ab").unwrap();
        session.write(b"cd").unwrap();
        assert_eq!(session.pending, b"abcd");
        let mut head = [0; 4];
        device.read_exact(&mut head).unwrap();
        assert_eq!(head, [0; 4]);
        session.seek(8).unwrap();
        session.flow.wait_idle().unwrap();
        assert!(session.pending.is_empty());
        device.seek(io::SeekFrom::Start(0)).unwrap();
        device.read_exact(&mut head).unwrap();
        assert_eq!(&head, b"abcd");
        session.write(b"ef").unwrap();
        session.fill([1, 2, 3, 4], 4).unwrap();
        session.write(b"gh").unwrap();
        assert_eq!(session.finish().unwrap(), 16);
        device.seek(io::SeekFrom::Start(8)).unwrap();
        let mut tail = [0; 8];
        device.read_exact(&mut tail).unwrap();
        assert_eq!(&tail, b"ef\x01\x02\x03\x04gh");
    }

    #[test]
    fn streams_two_frames_before_the_first_ack() {
        // The peer withholds every ACK until it has received both DATA frames.
        // Its alarm makes a stop-and-wait regression fail instead of hanging.
        let script = r#"
import signal, struct, sys, zlib
signal.alarm(10)
def exact(n):
    out = b''
    while len(out) < n:
        data = sys.stdin.buffer.read(n - len(out))
        if not data: raise EOFError()
        out += data
    return out
def receive():
    header = exact(9)
    assert header[:4] == b'FLSW'
    return header[4], exact(struct.unpack_from('<I', header, 5)[0])
def emit(op, payload):
    sys.stdout.buffer.write(b'FLSW' + bytes([op]) + struct.pack('<I', len(payload)) + payload)
    sys.stdout.buffer.flush()
size = 1 << 30
emit(0x81, struct.pack('<BQ', 1, size))
assert receive() == (10, b'')
emit(0x89, struct.pack('<Q', size))
frames = [receive(), receive()]
offset = 0
for op, payload in frames:
    assert op == 1
    n = struct.unpack_from('<I', payload)[0]
    assert len(payload) == n + 8
    assert zlib.crc32(payload[4:-4]) == struct.unpack_from('<I', payload, len(payload) - 4)[0]
    emit(0x82, struct.pack('<BQQ', 1, offset, n))
    offset += n
assert receive() == (4, b'')
emit(0x82, struct.pack('<BQQ', 4, offset, 0))
assert receive() == (5, b'')
emit(0x85, struct.pack('<Q', offset))
"#;
        let child = Command::new("python3")
            .args(["-u", "-c", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut session = Session::new(child, mpsc::unbounded_channel().0, 2 * CHUNK_SIZE).unwrap();
        session.write(b"first").unwrap();
        session.flush_pending().unwrap();
        session.write(b"second").unwrap();
        session.flush_pending().unwrap();
        assert_eq!(session.finish().unwrap(), 11);
    }

    #[test]
    fn in_flight_bytes_apply_backpressure_and_errors_wake_waiters() {
        use std::time::Duration;
        let flow = Arc::new(FlowControl::new(8));
        flow.reserve(6).unwrap();
        let (tx, rx) = std_mpsc::channel();
        let waiter = Arc::clone(&flow);
        let thread = std::thread::spawn(move || {
            tx.send(waiter.reserve(3)).unwrap();
        });
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Err(std_mpsc::RecvTimeoutError::Timeout)
        ));
        flow.complete(6);
        rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
        thread.join().unwrap();
        flow.complete(3);
        flow.reserve(8).unwrap();
        let (tx, rx) = std_mpsc::channel();
        let waiter = Arc::clone(&flow);
        let thread = std::thread::spawn(move || {
            tx.send(waiter.reserve(1)).unwrap();
        });
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Err(std_mpsc::RecvTimeoutError::Timeout)
        ));
        flow.fail(&io::Error::other("remote failed"));
        let error = rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap_err();
        assert_eq!(error.to_string(), "remote failed");
        thread.join().unwrap();
        assert!(flow.wait_idle().is_err());
    }

    #[test]
    fn native_crc_error_is_propagated() {
        let device = tempfile::NamedTempFile::new().unwrap();
        device.as_file().set_len(32).unwrap();
        let mut session = native_session(device.path());
        let mut frame = encode_data_frame(b"test").unwrap();
        *frame.last_mut().unwrap() ^= 1;
        session.queue_frame(OP_DATA, 0, 4, &frame).unwrap();
        let error = session.finish().unwrap_err();
        assert!(error.to_string().contains("CRC mismatch"), "{error}");
        assert!(error.to_string().contains("opcode 0x01"), "{error}");
    }
}
