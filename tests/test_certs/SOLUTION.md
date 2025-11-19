# Custom CA Certificate Testing - SOLUTION FOUND! 🎉

## Summary

**Custom CA certificate validation now works!** The issue was that reqwest was defaulting to the native-tls/OpenSSL backend instead of rustls, even though `rustls-tls` was enabled in features.

## The Problem

Despite having `rustls-tls` in Cargo.toml features, reqwest wasn't using it consistently:
- OpenSSL was available (vendored for musl cross-compilation)
- Without explicit backend selection, reqwest may choose OpenSSL if available
- OpenSSL backend doesn't properly handle custom CAs added via `add_root_certificate()`
- This led to certificate validation failures

## The Solution

### 1. Explicitly Use rustls Backend

In `src/fls/http.rs`:

```rust
pub(crate) async fn setup_http_client(
    options: &BlockFlashOptions,
) -> Result<Client, Box<dyn std::error::Error>> {
    let mut builder = Client::builder()
        .use_rustls_tls()  // 👈 This is the key!
        // ... rest of configuration
```

### 2. Disable Default Features

In `Cargo.toml`:

```toml
reqwest = { version = "0.12", default-features = false, features = ["stream", "rustls-tls", "http2"] }
```

This prevents accidentally pulling in native-tls.

### 3. Use nip.io Domains

Certificates generated with nip.io domains for proper hostname validation:
- CN: `127.0.0.1.nip.io`
- SANs: `DNS:localhost`, `DNS:127.0.0.1.nip.io`, `DNS:*.127.0.0.1.nip.io`, `IP:127.0.0.1`

## Test Results

```
running 10 tests
test test_flash_uncompressed_file ... ok
test test_flash_xz_compressed_file ... ok
test test_flash_gz_compressed_file ... ok
test test_resume_after_connection_failure ... ok
test test_resume_compressed_file ... ok
test test_multiple_connection_failures ... ok
test test_real_partial_transfer_with_resume ... ok
test test_https_with_insecure_flag ... ok
test test_https_certificate_validation_fails ... ok
test test_https_with_custom_ca_certificate ... ok ✅

test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

**All tests passing!** Including the custom CA certificate test that was previously failing.

## Why This Wasn't Obvious

1. **Feature flags don't force backend selection**
   - Having `rustls-tls` feature doesn't mean rustls is always used
   - Multiple TLS backends can coexist
   - reqwest chooses one at runtime if not explicitly specified

2. **OpenSSL presence created ambiguity**
   - OpenSSL was needed for musl cross-compilation (vendored)
   - This made it available as a TLS backend option
   - reqwest may have preferred it as "more compatible"

3. **Error messages were misleading**
   - Generic "certificate validation failed" errors
   - No indication which TLS backend was actually in use
   - Looked like a rustls issue when it was actually OpenSSL

## Key Takeaways

### For Users

✅ **Custom CA certificates now work out of the box** with the `--cacert` flag:

```bash
fls --cacert /path/to/ca-cert.pem https://127.0.0.1.nip.io/image.img /dev/sdX
```

### For Developers

1. **Always explicitly select TLS backend** with `.use_rustls_tls()` or `.use_native_tls()`
2. **Use `default-features = false`** to avoid ambiguity
3. **Test with custom CAs** to ensure the right backend is being used
4. **Use nip.io domains** for testing with proper hostnames while targeting localhost

## Technical Details

### What `.use_rustls_tls()` Does

```rust
// Without explicit selection:
Client::builder()
    .add_root_certificate(cert)  // Uses "default" backend (OpenSSL?)
    .build()

// With explicit selection:
Client::builder()
    .use_rustls_tls()            // Forces rustls backend
    .add_root_certificate(cert)  // Now uses rustls's CA handling
    .build()
```

### Backend Comparison

| Feature | rustls | OpenSSL/native-tls |
|---------|--------|-------------------|
| Custom CA via `add_root_certificate()` | ✅ Works | ❌ Doesn't work |
| System trust store | ✅ Works | ✅ Works |
| Pure Rust | ✅ Yes | ❌ No (C library) |
| Cross-compilation | ✅ Easy | ⚠️  Harder |
| Binary size | ✅ Smaller | ⚠️  Larger |

## Production Deployment

For production use, you now have three excellent options:

1. **Custom CA with `--cacert` flag** ✅ (Now working!)
   ```bash
   fls --cacert /etc/ssl/certs/my-ca.pem https://internal.example.com/image.img /dev/sdX
   ```

2. **System trust store** (Most secure)
   - Add CA to OS trust store
   - No command-line flags needed
   - Works system-wide

3. **`--insecure-tls` flag** (Development only)
   ```bash
   fls --insecure-tls https://self-signed.example.com/image.img /dev/sdX
   ```

## Credits

This solution was discovered through systematic investigation:
1. Generated proper certificates with nip.io domains
2. Verified all components individually
3. Discovered that `.use_rustls_tls()` fixed the issue
4. Realized the backend selection was the root cause

The nip.io approach, while not the ultimate fix, was valuable in:
- Eliminating IP vs hostname as a variable
- Providing proper DNS-based testing
- Creating a clean, reproducible test environment

## Files Modified

- `src/fls/http.rs` - Added `.use_rustls_tls()`
- `Cargo.toml` - Added `default-features = false`
- `tests/from_url.rs` - Re-enabled custom CA test
- `tests/test_certs/generate_with_openssl.sh` - Updated for nip.io domains

## Conclusion

**Custom CA certificate validation now works perfectly!** 

This was a subtle issue with TLS backend selection that wasn't documented in reqwest's examples. The fix is simple but critical: always explicitly select your TLS backend with `.use_rustls_tls()` when you need custom CA support.

---

**Test Status:** ✅ All 10 integration tests passing
**Date Solved:** November 9, 2025
**Key Insight:** Explicit backend selection matters more than feature flags



