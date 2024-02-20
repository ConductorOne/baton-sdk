# xjwt: small extensions for implementing JWT-based systems

[![GoDoc](https://godoc.org/github.com/pquerna/xjwt?status.svg)](https://godoc.org/github.com/pquerna/xjwt)
[![Build Status](https://travis-ci.org/pquerna/xjwt.svg?branch=main)](https://travis-ci.org/pquerna/xjwt)

# Methods

## JWT Verification

 `xjwt.Verify` and  `xjwt.VerifyRaw` are strict verifying methods for validating a JWT is valid and well formed.

## Remote JWK Keysets

`xkeyset.RemoteKeyset` wraps a remote JWKs URL, caching and refreshing a list of JWKs in the background.

## jose.v2 shortcuts

### RandomNonce

`xjwt.RandomNonce` provides a basic, random value, conforming to the `jose.NonceSource` interface.

### Converting PEM encoding to JOSE types

`xjwt.ParsePrivateKey` converts a private key from a PEM encoding to a `*jose.JSONWebKey`.

# License

`xjwt` is licensed under the Apache License Version 2.0. See the [LICENSE file](./LICENSE) for details.

`pquerna/xjwt` is a fork of unmaintained [`ScaleFT/xjwt`](https://github.com/ScaleFT/xjwt)