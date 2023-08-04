__all__ = [
	"bool_",
	"int8",
	"int16",
	"int32",
	"int64",
	"uint8",
	"uint16",
	"uint32",
	"uint64",
	"float32",
	"float64",
	"timestamp",
	"duration",
	"string",
	"DataType",
	"BoolType",
	"Int8Type",
	"Int16Type",
	"Int32Type",
	"Int64Type",
	"UInt8Type",
	"UInt16Type",
	"UInt32Type",
	"UInt64Type",
	"Float32Type",
	"Float64Type",
	"TimestampType",
	"DurationType",
	"StringType",
]

from ella._internal import (
	DataType,
	BoolType,
	Int8Type,
	Int16Type,
	Int32Type,
	Int64Type,
	UInt8Type,
	UInt16Type,
	UInt32Type,
	UInt64Type,
	Float32Type,
	Float64Type,
	TimestampType,
	DurationType,
	StringType,
)

bool_: BoolType = BoolType()
int8: Int8Type = Int8Type()
int16: Int16Type = Int16Type()
int32: Int32Type = Int32Type()
int64: Int64Type = Int64Type()
uint8: UInt8Type = UInt8Type()
uint16: UInt16Type = UInt16Type()
uint32: UInt32Type = UInt32Type()
uint64: UInt64Type = UInt64Type()
float32: Float32Type = Float32Type()
float64: Float64Type = Float64Type()
timestamp: TimestampType = TimestampType()
duration: DurationType = DurationType()
string: StringType = StringType()
