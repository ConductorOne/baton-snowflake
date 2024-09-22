package snowflake

import (
	"fmt"
	"strconv"
	"time"
)

/**
TIME, TIMESTAMP_LTZ, TIMESTAMP_NTZ

    Float value (with 9 decimal places) of the number of seconds since the epoch (e.g. 82919.000000000).
TIMESTAMP_TZ

    Float value (with 9 decimal places) of the number of seconds since the epoch, followed by a space and the time zone offset in minutes (e.g. 1616173619000000000 960)

*/

func parseTime(input string) (time.Time, error) {
	// Step 1: Parse the string into a float64
	floatValue, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse float value: %v", err)
	}

	// Step 2: Separate the integer and fractional parts
	seconds := int64(floatValue)
	nanoseconds := int64((floatValue - float64(seconds)) * 1e9)

	// Step 3: Use time.Unix to create a time.Time value
	timestamp := time.Unix(seconds, nanoseconds).UTC() // Use UTC for NTZ
	return timestamp, nil
}
