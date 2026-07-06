//go:build windows

package field

var logEventLogField = BoolField(LogEventLogFieldName,
	WithDescription("Log to the Windows event log instead of a file"),
	WithPersistent(true),
	WithExportTarget(ExportTargetNone))

var platformDefaultFields = []SchemaField{
	logEventLogField,
}
