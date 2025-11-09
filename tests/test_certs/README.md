# Test Certificates for HTTPS Testing

This directory contains test certificates used for integration testing of HTTPS functionality.

## Files

- `ca-cert.pem` - Root CA certificate (valid for 100 years)
- `ca-key.pem` - Root CA private key
- `server-cert.pem` - Server certificate signed by CA (valid for 100 years)
- `server-key.pem` - Server private key

## Generating Certificates

To regenerate these certificates, run:

```bash
cargo run --example generate_test_certs
```

Or use the generate_certs.rs script in this directory.

## Security Note

**DO NOT use these certificates in production!** They are only for testing purposes and have:
- Long expiry periods (100 years)
- Predictable generation
- Committed private keys

These certificates are safe to commit to version control as they are only for testing.

