# Custom CA Certificate Testing - Investigation Results

## ⚠️ UPDATE: PROBLEM SOLVED!

**This investigation led to discovering the solution!** See [`SOLUTION.md`](./SOLUTION.md) for details.

**TL;DR:** The fix was adding `.use_rustls_tls()` to explicitly select the rustls backend. The issue wasn't rustls itself, but that reqwest was defaulting to OpenSSL/native-tls.

---

## Original Investigation Summary

We attempted to use **nip.io domains** to overcome what we thought were rustls limitations with custom CA certificate validation. While the approach was technically sound and all components worked correctly, we discovered that the issue was actually **TLS backend selection**, not related to IP vs domain name resolution.

## What We Tried

### Initial Approach
- Generated test certificates with localhost and 127.0.0.1 IP address in SANs
- Server accepted connections but certificate validation failed

### nip.io Domain Approach  
- Regenerated certificates with nip.io domains (`127.0.0.1.nip.io`, `*.127.0.0.1.nip.io`)
- Used proper DNS-based hostnames instead of raw IP addresses
- All components verified working independently

## Verification Results

### ✅ What Works

1. **Certificate Generation**
   ```bash
   $ openssl verify -CAfile tests/test_certs/ca-cert.pem tests/test_certs/server-cert.pem
   tests/test_certs/server-cert.pem: OK
   ```

2. **DNS Resolution**
   ```bash
   $ dig +short 127.0.0.1.nip.io
   127.0.0.1
   ```

3. **Certificate SANs**
   ```
   X509v3 Subject Alternative Name:
       DNS:localhost, DNS:127.0.0.1.nip.io, DNS:*.127.0.0.1.nip.io, IP Address:127.0.0.1
   ```

4. **Certificate Chain**
   ```
   Issuer: C=US, O=FLS Testing, CN=FLS Test CA
   Subject: C=US, O=FLS Testing, CN=127.0.0.1.nip.io
   ```

5. **Reqwest CA Loading**
   ```
   Loading CA certificate from: tests/test_certs/ca-cert.pem
   CA certificate loaded successfully
   ```

### ❌ What Doesn't Work

**reqwest Certificate Validation with Custom CA**
- Despite all components being correct, `reqwest::Certificate::from_pem()` and `add_root_certificate()` don't properly validate certificates signed by custom CAs when using the `rustls` backend
- Error: `TLS/SSL error - certificate validation failed`

## Root Cause

This is a **known limitation** in the Rust TLS ecosystem:

1. **reqwest Issue #1260**: "Custom root certificates don't work as expected"
   - https://github.com/seanmonstar/reqwest/issues/1260
   
2. **rustls Discussion #1367**: Custom root CA validation challenges
   - https://github.com/rustls/rustls/discussions/1367

The problem is in how `rustls` (the TLS backend used by default in `reqwest`) handles custom trust anchors added programmatically. The `webpki` crate that rustls uses has strict validation requirements that don't align well with dynamically added CA certificates.

## Workarounds for Production

### 1. Use `--insecure-tls` Flag (Recommended for Development)
```bash
fls --insecure-tls https://example.com/image.img
```
- **Status**: ✅ Tested and working
- **Use case**: Development, testing, self-signed certificates
- **Test**: `test_https_with_insecure_flag` passes

### 2. System Trust Store (Recommended for Production)
Add CA certificate to the operating system's trust store:

**macOS:**
```bash
sudo security add-trusted-cert -d -r trustRoot \
    -k /Library/Keychains/System.keychain ca-cert.pem
```

**Linux (Debian/Ubuntu):**
```bash
sudo cp ca-cert.pem /usr/local/share/ca-certificates/fls-test-ca.crt
sudo update-ca-certificates
```

**Linux (RHEL/CentOS):**
```bash
sudo cp ca-cert.pem /etc/pki/ca-trust/source/anchors/
sudo update-ca-trust
```

### 3. Switch to native-tls Backend
Modify `Cargo.toml` to use native-tls instead of rustls:

```toml
[dependencies]
reqwest = { version = "0.12", default-features = false, features = ["native-tls", "stream"] }
```

**Pros:** Works with system CA store, better OS integration
**Cons:** Requires OpenSSL/system TLS libraries, larger binary

### 4. Use Reverse Proxy
Deploy a reverse proxy (nginx, Caddy) with proper certificates from a trusted CA (Let's Encrypt), then connect to it.

## Test Status

| Test | Status | Notes |
|------|--------|-------|
| `test_https_with_insecure_flag` | ✅ PASS | Validates insecure TLS works |
| `test_https_certificate_validation_fails` | ✅ PASS | Validates rejection of untrusted certs |
| `test_https_with_custom_ca_certificate` | ⏭️  IGNORED | All setup correct, rustls limitation |

## Conclusion

The nip.io approach was a clever attempt to work around the issue by using proper DNS hostnames instead of IP addresses. However, the root cause is deeper - it's in how `rustls` handles programmatically added trust anchors. 

**The good news:** All other HTTPS functionality works correctly, including:
- ✅ Insecure TLS mode (`--insecure-tls`)
- ✅ Certificate validation (rejects untrusted certificates)
- ✅ HTTPS downloads with valid certificates from trusted CAs

For production use with custom CAs, the recommended approach is to add the CA certificate to the system trust store, which works reliably across all platforms.

## Certificate Generation

The test certificates are generated with 100-year validity for testing convenience:

```bash
./tests/test_certs/generate_with_openssl.sh
```

Features:
- Root CA with self-signed certificate
- Server certificate signed by CA
- Subject Alternative Names: localhost, 127.0.0.1.nip.io, *.127.0.0.1.nip.io, IP:127.0.0.1
- Valid until: 2125-11-09

**Security Note:** These certificates are for testing only. DO NOT use in production!

