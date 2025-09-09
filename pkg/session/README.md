# Session Cache Implementation

This package provides session cache implementations for the Baton SDK. It includes both in-memory and gRPC-based implementations.

## Overview

The session cache is used to store temporary data during sync operations. It provides a key-value store interface with support for:

- Basic CRUD operations (Get, Set, Delete, Clear)
- Batch operations (GetMany, SetMany)
- Namespace isolation using sync IDs
- Prefix support for key organization
- Context-based configuration
- An implemention will be chosen at runtime (the grpc interface will be used if the build tag is specified).