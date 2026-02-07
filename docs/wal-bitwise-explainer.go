// +build ignore

// Run with: go run docs/wal-bitwise-explainer.go
//
// This file explains the bitwise operations in the WAL implementation.
package main

import (
	"fmt"
)

func main() {
	fmt.Println("=== WAL Bitwise Operations Explainer ===")
	fmt.Println()

	// ============================================================
	// PART 1: Why 8-byte alignment?
	// ============================================================
	fmt.Println("PART 1: Why 8-byte alignment?")
	fmt.Println("─────────────────────────────")
	fmt.Println(`
Disk sectors are typically 512 bytes. If we write data that spans
sector boundaries and power fails mid-write, we get a "torn write":
- Some sectors have new data
- Some sectors have old data (or zeros)

By aligning our length field to 8 bytes (a common atomic write size),
we ensure the length is NEVER partially written. Either we have the
full length, or we have zeros/old data - never garbage.
`)

	// ============================================================
	// PART 2: The encoding scheme
	// ============================================================
	fmt.Println("PART 2: The encoding scheme")
	fmt.Println("───────────────────────────")
	fmt.Println(`
We have 8 bytes (64 bits) for our length field:
- Lower 56 bits: actual data length (max ~72 petabytes - plenty!)
- Upper 8 bits: padding information (0x80 | padBytes)

Why encode padding? So we know exactly how many bytes to skip
when reading back.
`)

	// ============================================================
	// PART 3: encodeFrameSize - step by step
	// ============================================================
	fmt.Println("PART 3: encodeFrameSize() - encoding examples")
	fmt.Println("──────────────────────────────────────────────")

	examples := []int{16, 20, 23, 24, 100}
	for _, dataBytes := range examples {
		lenField, padBytes := encodeFrameSize(dataBytes)
		fmt.Printf("\nData size: %d bytes\n", dataBytes)
		fmt.Printf("  Step 1: padBytes = (8 - (%d %% 8)) %% 8 = (8 - %d) %% 8 = %d\n",
			dataBytes, dataBytes%8, padBytes)
		fmt.Printf("  Step 2: lenField starts as data length = %d (0x%016X)\n",
			dataBytes, uint64(dataBytes))

		if padBytes != 0 {
			paddingByte := 0x80 | padBytes
			fmt.Printf("  Step 3: Padding needed! paddingByte = 0x80 | %d = 0x%02X\n",
				padBytes, paddingByte)
			fmt.Printf("  Step 4: Shift to upper byte: 0x%02X << 56 = 0x%016X\n",
				paddingByte, uint64(paddingByte)<<56)
			fmt.Printf("  Step 5: OR with length: 0x%016X | 0x%016X = 0x%016X\n",
				uint64(dataBytes), uint64(paddingByte)<<56, lenField)
		} else {
			fmt.Printf("  Step 3: No padding needed (already 8-byte aligned)\n")
			fmt.Printf("  Step 4: lenField = %d (0x%016X)\n", lenField, lenField)
		}
		fmt.Printf("  Result: lenField=0x%016X, padBytes=%d, total=%d (8-byte aligned: %v)\n",
			lenField, padBytes, dataBytes+padBytes, (dataBytes+padBytes)%8 == 0)
	}

	// ============================================================
	// PART 4: Why 0x80?
	// ============================================================
	fmt.Println("\n\nPART 4: Why 0x80 in the padding byte?")
	fmt.Println("─────────────────────────────────────")
	fmt.Println(`
0x80 = 1000 0000 in binary (the high bit is set)

This serves as a FLAG to indicate "padding is present":
- If the top byte is 0x00: no padding (data was already aligned)
- If the top byte has 0x80 set: padding exists, lower 3 bits = count

The padding count (0-7) only needs 3 bits, so we have room for the flag.

Examples:
  0x00 = 0000 0000 = no padding
  0x81 = 1000 0001 = 1 byte of padding
  0x82 = 1000 0010 = 2 bytes of padding
  ...
  0x87 = 1000 0111 = 7 bytes of padding
`)

	// ============================================================
	// PART 5: decodeFrameSize - step by step
	// ============================================================
	fmt.Println("PART 5: decodeFrameSize() - decoding examples")
	fmt.Println("──────────────────────────────────────────────")

	// Decode the same examples
	for _, dataBytes := range examples {
		lenField, _ := encodeFrameSize(dataBytes)
		recBytes, padBytes := decodeFrameSize(int64(lenField))

		fmt.Printf("\nEncoded lenField: 0x%016X\n", lenField)
		fmt.Printf("  Step 1: Extract lower 56 bits for data length\n")
		fmt.Printf("          Mask: ^(0xFF << 56) = 0x%016X\n", ^(uint64(0xFF) << 56))
		fmt.Printf("          lenField & mask = 0x%016X & 0x%016X = %d\n",
			lenField, ^(uint64(0xFF)<<56), recBytes)

		fmt.Printf("  Step 2: Check if padding exists (is lenField negative as int64?)\n")
		fmt.Printf("          int64(0x%016X) = %d (negative: %v)\n",
			lenField, int64(lenField), int64(lenField) < 0)

		if int64(lenField) < 0 {
			fmt.Printf("  Step 3: Extract padding from upper byte\n")
			fmt.Printf("          (lenField >> 56) & 0x7 = (0x%02X) & 0x7 = %d\n",
				(lenField >> 56), padBytes)
		} else {
			fmt.Printf("  Step 3: No padding (lenField is positive)\n")
		}
		fmt.Printf("  Result: recBytes=%d, padBytes=%d\n", recBytes, padBytes)
	}

	// ============================================================
	// PART 6: The "negative" trick explained
	// ============================================================
	fmt.Println("\n\nPART 6: Why checking 'lenField < 0' works")
	fmt.Println("──────────────────────────────────────────")
	fmt.Println(`
In two's complement (how computers represent signed integers):
- If the highest bit (bit 63) is 0: the number is positive
- If the highest bit (bit 63) is 1: the number is negative

Our 0x80 flag sets bit 7 of the upper byte, which is bit 63 overall!

So when we set 0x80 in the upper byte:
  0x80 << 56 = 0x8000000000000000

This sets bit 63, making the int64 interpretation NEGATIVE.

This is a clever trick: instead of masking and checking a flag bit,
we just check if the value is negative!
`)

	// Demonstrate
	fmt.Println("Demonstration:")
	nopad := uint64(100)                  // No padding
	withpad := uint64(100) | (0x82 << 56) // With 2 bytes padding

	fmt.Printf("  No padding:   0x%016X as int64 = %d (positive)\n", nopad, int64(nopad))
	fmt.Printf("  With padding: 0x%016X as int64 = %d (negative!)\n", withpad, int64(withpad))

	// ============================================================
	// PART 7: The complete wire format
	// ============================================================
	fmt.Println("\n\nPART 7: Complete wire format example")
	fmt.Println("─────────────────────────────────────")

	// Let's trace through a complete record
	recordType := int64(1)
	data := []byte("Hello!")
	fmt.Printf("Record: Type=%d, Data=%q (%d bytes)\n", recordType, data, len(data))

	// Simulate what the encoder does
	// Record format: Type(8) + CRC(4) + Len(4) + Data
	recordSize := 8 + 4 + 4 + len(data) // 16 + 6 = 22 bytes
	fmt.Printf("\nRecord binary size: 8 (type) + 4 (crc) + 4 (len) + %d (data) = %d bytes\n",
		len(data), recordSize)

	lenField, padBytes := encodeFrameSize(recordSize)
	fmt.Printf("\nWire format:\n")
	fmt.Printf("  ┌─────────────────────────────────────────────────────────────┐\n")
	fmt.Printf("  │ Length Field: 0x%016X (8 bytes)                  │\n", lenField)
	fmt.Printf("  ├─────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("  │ Record Data: %d bytes                                       │\n", recordSize)
	fmt.Printf("  │   - Type: %d (8 bytes)                                      │\n", recordType)
	fmt.Printf("  │   - CRC:  <computed> (4 bytes)                              │\n")
	fmt.Printf("  │   - Len:  %d (4 bytes)                                       │\n", len(data))
	fmt.Printf("  │   - Data: %q (%d bytes)                              │\n", data, len(data))
	fmt.Printf("  ├─────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("  │ Padding: %d bytes of zeros                                   │\n", padBytes)
	fmt.Printf("  └─────────────────────────────────────────────────────────────┘\n")
	fmt.Printf("  Total: 8 + %d + %d = %d bytes (divisible by 8: %v)\n",
		recordSize, padBytes, 8+recordSize+padBytes, (8+recordSize+padBytes)%8 == 0)
}

// encodeFrameSize computes the length field and padding bytes.
// Copy from encoder.go for demonstration.
func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// Force 8-byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}

// decodeFrameSize extracts the record size and padding from the length field.
// Copy from decoder.go for demonstration.
func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	// The record size is stored in the lower 56 bits
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	// Non-zero padding is indicated by set MSB (negative length)
	if lenField < 0 {
		// Padding is stored in lower 3 bits of length MSB
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return recBytes, padBytes
}
