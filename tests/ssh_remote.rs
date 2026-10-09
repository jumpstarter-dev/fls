use flate2::{write::GzEncoder, Compression};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn http_cli_uploads_and_flashes_with_all_ssh_auth_modes() {
    let dir = tempfile::tempdir().unwrap();
    let native = dir.path().join("native-write-head");
    assert!(std::process::Command::new("cc")
        .args(["-O2", "remote/fls-wh.c", "-o"])
        .arg(&native)
        .status()
        .unwrap()
        .success());

    // Stand in for SSH locally; each subprocess has its own PATH and credentials.
    // Keep uploads inside the test directory rather than the host's /tmp.
    let ssh = dir.path().join("ssh");
    std::fs::write(
        &ssh,
        r#"#!/bin/sh
if [ "$1" = -C ]; then echo compression >> "$SSH_LOG"; shift; fi
if [ "$1" = -p ]; then
  [ "$2" = 11223 ] || exit 48
  echo port >> "$SSH_LOG"
  shift 2
fi
if [ "$STRICT_CHECK" = 1 ]; then
  [ "$1" = -o ] && [ "$2" = StrictHostKeyChecking=yes ] || exit 49
  shift 2
  echo strict >> "$SSH_LOG"
else
  [ "$1" = -o ] && [ "$2" = StrictHostKeyChecking=no ] || exit 49
  shift 2
  [ "$1" = -o ] && [ "$2" = UserKnownHostsFile=/dev/null ] || exit 53
  shift 2
  [ "$1" = -o ] && [ "$2" = LogLevel=ERROR ] || exit 54
  shift 2
  echo unchecked >> "$SSH_LOG"
fi
if [ "$1" = -o ]; then
  [ "$2" = BatchMode=yes ] || exit 50
  [ -z "$SSHPASS$FLS_SSH_PASS_FILE" ] || exit 51
  echo batch >> "$SSH_LOG"
  shift 2
else
  [ -n "$SSHPASS$FLS_SSH_PASS_FILE" ] || exit 52
fi
[ "$1" = -T ] || exit 40
shift
[ "$1" = -- ] || exit 41
shift
[ "$1" = root@fake-host ] || exit 42
shift
case "$1" in
  *"uname -s"*)
    echo probe >> "$SSH_LOG"
    [ "$PROBE_FAIL" != 1 ] || exit 47
    if [ "$HOST_KEY_UNKNOWN" = 1 ] && [ "$STRICT_CHECK" = 1 ]; then
      echo 'Host key verification failed.' >&2
      exit 255
    fi
    printf '%s\n' Linux x86_64 6.1.0 ;;
  PATH=*) cat > "$WH_UPLOAD" && chmod +x "$WH_UPLOAD" ;;
  /tmp/fls-wh\ *) command=${1#/tmp/fls-wh}; exec sh -c "\"$WH_UPLOAD\"$command" ;;
  *) exit 43 ;;
esac
"#,
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let sshpass = dir.path().join("sshpass");
    std::fs::write(
        &sshpass,
        r#"#!/bin/sh
case "$1" in
  -f) [ -r "$2" ] || exit 44; echo file >> "$SSH_LOG"; shift 2 ;;
  -e) [ -n "$SSHPASS" ] || exit 45; echo env >> "$SSH_LOG"; shift ;;
  *) exit 46 ;;
esac
exec "$@"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&sshpass, std::fs::Permissions::from_mode(0o755)).unwrap();
    let password = dir.path().join("password");
    std::fs::write(&password, "test-password\n").unwrap();

    let server = MockServer::start().await;
    let image: Vec<u8> = (0..(1 << 20) + 17).map(|i| (i % 251) as u8).collect();
    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    gzip.write_all(&image).unwrap();
    Mock::given(method("GET"))
        .and(path("/image.img.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(gzip.finish().unwrap()))
        .mount(&server)
        .await;

    for auth in [
        "key",
        "env",
        "file",
        "config-port",
        "oversize",
        "probe-fail",
        "unknown-key",
        "strict-key",
        "strict-env",
        "strict-file",
        "strict-unknown-key",
    ] {
        let device = dir.path().join(format!("block '{auth};device"));
        let file = std::fs::File::create(&device).unwrap();
        file.set_len(if auth == "oversize" {
            1024
        } else {
            (image.len() + 1024) as u64
        })
        .unwrap();
        let log = dir.path().join(format!("{auth}.log"));
        let upload = dir.path().join(format!("{auth}.uploaded"));
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fls"));
        command
            .args([
                "from-url",
                &format!("{}/image.img.gz", server.uri()),
                &format!("root@fake-host:{}", device.display()),
                "--ssh-compress",
                "--wh-bin",
            ])
            .arg(&native)
            .arg("--newline-progress")
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    dir.path().display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env("WH_UPLOAD", &upload)
            .env("SSH_LOG", &log)
            .env_remove("SSHPASS")
            .env_remove("FLS_SSH_PASS_FILE")
            .env_remove("FLS_WH_BIN")
            .env_remove("PROBE_FAIL")
            .env_remove("HOST_KEY_UNKNOWN")
            .env_remove("STRICT_CHECK")
            .kill_on_drop(true);
        if auth != "config-port" {
            command.args(["--ssh-port", "11223"]);
        }
        if auth.starts_with("strict-") {
            command
                .arg("--strict-ssh-host-key-checking")
                .env("STRICT_CHECK", "1");
        }
        match auth {
            "env" | "strict-env" => {
                command.env("SSHPASS", "test-password");
            }
            "file" | "strict-file" => {
                // File authentication must win even when SSHPASS is set.
                command
                    .env("SSHPASS", "ignored-password")
                    .env("FLS_SSH_PASS_FILE", &password);
            }
            "probe-fail" => {
                command
                    .env("PROBE_FAIL", "1")
                    .args(["--write-buffer-size", "8"]);
            }
            "unknown-key" | "strict-unknown-key" => {
                command.env("HOST_KEY_UNKNOWN", "1");
            }
            _ => {}
        }
        let output = tokio::time::timeout(Duration::from_secs(30), command.output())
            .await
            .unwrap()
            .unwrap();
        if matches!(auth, "probe-fail" | "strict-unknown-key") {
            assert!(!output.status.success());
            assert!(!upload.exists(), "Platform detection must precede upload");
            assert!(
                String::from_utf8_lossy(&output.stderr).contains("SSH to root@fake-host failed")
            );
            if auth == "strict-unknown-key" {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // The inline error report and the final summary can both carry
                // the diagnostic; the inline one races the process exit, so 1
                // or 2.
                let count = stderr.matches("Host key verification failed").count();
                assert!(
                    (1..=2).contains(&count),
                    "host key failure reported {count} times"
                );
                assert!(stderr.contains("ssh -p 11223 'root@fake-host'"));
                assert!(std::fs::read(&device)
                    .unwrap()
                    .iter()
                    .all(|&byte| byte == 0));
            }
            continue;
        }
        if auth == "oversize" {
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains("exceeds remote device size"));
            assert_eq!(std::fs::metadata(&device).unwrap().len(), 1024);
            continue;
        }
        assert!(
            output.status.success(),
            "{auth}: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(&std::fs::read(&device).unwrap()[..image.len()], &image);
        assert_eq!(
            std::fs::read(upload).unwrap(),
            std::fs::read(&native).unwrap()
        );
        let log = std::fs::read_to_string(log).unwrap();
        assert_eq!(log.matches("compression").count(), 3);
        assert_eq!(
            log.matches("port").count(),
            if auth == "config-port" { 0 } else { 3 }
        );
        assert_eq!(log.matches("probe").count(), 1);
        assert_eq!(
            log.matches("strict").count(),
            if auth.starts_with("strict-") { 3 } else { 0 }
        );
        assert_eq!(
            log.matches("unchecked").count(),
            if auth.starts_with("strict-") { 0 } else { 3 }
        );
        assert_eq!(
            log.matches("batch").count(),
            if matches!(auth, "file" | "env" | "strict-file" | "strict-env") {
                0
            } else {
                3
            }
        );
        assert_eq!(
            log.matches("file").count(),
            if matches!(auth, "file" | "strict-file") {
                3
            } else {
                0
            }
        );
        assert_eq!(
            log.matches("env").count(),
            if matches!(auth, "env" | "strict-env") {
                3
            } else {
                0
            }
        );
    }
}
