#!/bin/bash
set -e

# Script to generate test certificates using OpenSSL
# These certificates are valid for 100 years and are for testing only

CERT_DIR="tests/test_certs"
cd "$(dirname "$0")/../.."

echo "Generating test certificates for HTTPS testing..."

# Generate CA private key
openssl ecparam -genkey -name prime256v1 -out "$CERT_DIR/ca-key.pem"

# Generate CA certificate (self-signed, 100 years validity)
openssl req -new -x509 -days 36500 -key "$CERT_DIR/ca-key.pem" -out "$CERT_DIR/ca-cert.pem" \
    -subj "/C=US/O=FLS Testing/CN=FLS Test CA"

echo "✓ Generated CA certificate: $CERT_DIR/ca-cert.pem"

# Generate server private key
openssl ecparam -genkey -name prime256v1 -out "$CERT_DIR/server-key.pem"

# Create server certificate signing request
# Using nip.io domain as CN for better compatibility
openssl req -new -key "$CERT_DIR/server-key.pem" -out "$CERT_DIR/server.csr" \
    -subj "/C=US/O=FLS Testing/CN=127.0.0.1.nip.io"

# Create extensions file for SAN
# Using nip.io domains for better compatibility with rustls/reqwest
cat > "$CERT_DIR/server-ext.cnf" << EOF
subjectAltName = DNS:localhost,DNS:127.0.0.1.nip.io,DNS:*.127.0.0.1.nip.io,IP:127.0.0.1
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
EOF

# Sign server certificate with CA (100 years validity)
openssl x509 -req -in "$CERT_DIR/server.csr" -CA "$CERT_DIR/ca-cert.pem" \
    -CAkey "$CERT_DIR/ca-key.pem" -CAcreateserial -days 36500 \
    -out "$CERT_DIR/server-cert.pem" -extfile "$CERT_DIR/server-ext.cnf"

echo "✓ Generated server certificate: $CERT_DIR/server-cert.pem"

# Clean up temporary files
rm -f "$CERT_DIR/server.csr" "$CERT_DIR/server-ext.cnf" "$CERT_DIR/ca-cert.srl"

# Verify certificate chain
echo ""
echo "Verifying certificate chain..."
openssl verify -CAfile "$CERT_DIR/ca-cert.pem" "$CERT_DIR/server-cert.pem"

echo ""
echo "Certificates generated successfully!"
echo "Valid for 100 years (until $(date -v+100y '+%Y-%m-%d' 2>/dev/null || date -d '+100 years' '+%Y-%m-%d' 2>/dev/null || echo '2124'))."
echo ""
echo "These certificates are for testing only - DO NOT use in production!"

