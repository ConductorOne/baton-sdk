/-!
# Shared primitive types

Bytes are modeled as `Nat`. Production bytes lie in `0..255`; every
theorem in this package quantifies over all naturals and therefore
holds on that subset. The oracle generator emits only values below 256.
-/

namespace C1z

/-- A byte. See the module docstring for why this is `Nat`. -/
abbrev Byte := Nat

/-- A byte string, such as a Go `string` or `[]byte` viewed as raw bytes. -/
abbrev Bytes := List Byte

end C1z
