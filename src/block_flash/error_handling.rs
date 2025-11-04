use tokio::sync::mpsc;

pub(crate) async fn process_error_messages(
    mut error_rx: mpsc::UnboundedReceiver<String>,
) {
    while let Some(error_msg) = error_rx.recv().await {
        eprintln!("{}", error_msg);
    }
}

