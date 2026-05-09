package codes

// Code represents a Postgres error code
type Code string

// http://www.postgresql.org/docs/9.5/static/errcodes-appendix.html.
var (
	// Section: Class 00 - Successful Completion
	SuccessfulCompletion Code = "00000"
	// Section: Class 01 - Warning
	Warning                                 Code = "01000"
	WarningDynamicResultSetsReturned        Code = "0100C"
	WarningImplicitZeroBitPadding           Code = "01008"
	WarningNullValueEliminatedInSetFunction Code = "01003"
	WarningPrivilegeNotGranted              Code = "01007"
	WarningPrivilegeNotRevoked              Code = "01006"
	WarningStringDataRightTruncation        Code = "01004"
	WarningDeprecatedFeature                Code = "01P01"
	// Section: Class 02 - No Data (this is also a warning class per the SQL standard)
	NoData                                Code = "02000"
	NoAdditionalDynamicResultSetsReturned Code = "02001"
	// Section: Class 03 - SQL Statement Not Yet Complete
	SQLStatementNotYetComplete Code = "03000"
	// Section: Class 08 - Connection Exception
	ConnectionException                           Code = "08000"
	ConnectionDoesNotExist                        Code = "08003"
	ConnectionFailure                             Code = "08006"
	SQLclientUnableToEstablishSQLconnection       Code = "08001"
	SQLserverRejectedEstablishmentOfSQLconnection Code = "08004"
	TransactionResolutionUnknown                  Code = "08007"
	ProtocolViolation                             Code = "08P01"
	// Section: Class 09 - Triggered Action Exception
	TriggeredActionException Code = "09000"
	// Section: Class 0A - Feature Not Supported
	FeatureNotSupported Code = "0A000"
	// Section: Class 0B - Invalid Transaction Initiation
	InvalidTransactionInitiation Code = "0B000"
	// Section: Class 0F - Locator Exception
	LocatorException            Code = "0F000"
	InvalidLocatorSpecification Code = "0F001"
	// Section: Class 0L - Invalid Grantor
	InvalidGrantor        Code = "0L000"
	InvalidGrantOperation Code = "0LP01"
	// Section: Class 0P - Invalid Role Specification
	InvalidRoleSpecification Code = "0P000"
	// Section: Class 0Z - Diagnostics Exception
	DiagnosticsException                           Code = "0Z000"
	StackedDiagnosticsAccessedWithoutActiveHandler Code = "0Z002"
	// Section: Class 20 - Case Not Found
	CaseNotFound Code = "20000"
	// Section: Class 21 - Cardinality Violation
	CardinalityViolation Code = "21000"
	// Section: Class 22 - Data Exception
	DataException                         Code = "22000"
	ArraySubscript                        Code = "2202E"
	CharacterNotInRepertoire              Code = "22021"
	DatetimeFieldOverflow                 Code = "22008"
	DivisionByZero                        Code = "22012"
	InvalidWindowFrameOffset              Code = "22013"
	ErrorInAssignment                     Code = "22005"
	EscapeCharacterConflict               Code = "2200B"
	IndicatorOverflow                     Code = "22022"
	IntervalFieldOverflow                 Code = "22015"
	InvalidArgumentForLogarithm           Code = "2201E"
	InvalidArgumentForNtileFunction       Code = "22014"
	InvalidArgumentForNthValueFunction    Code = "22016"
	InvalidArgumentForPowerFunction       Code = "2201F"
	InvalidArgumentForWidthBucketFunction Code = "2201G"
	InvalidCharacterValueForCast          Code = "22018"
	InvalidDatetimeFormat                 Code = "22007"
	InvalidEscapeCharacter                Code = "22019"
	InvalidEscapeOctet                    Code = "2200D"
	InvalidEscapeSequence                 Code = "22025"
	NonstandardUseOfEscapeCharacter       Code = "22P06"
	InvalidIndicatorParameterValue        Code = "22010"
	InvalidParameterValue                 Code = "22023"
	InvalidRegularExpression              Code = "2201B"
	InvalidRowCountInLimitClause          Code = "2201W"
	InvalidRowCountInResultOffsetClause   Code = "2201X"
	InvalidTimeZoneDisplacementValue      Code = "22009"
	InvalidUseOfEscapeCharacter           Code = "2200C"
	MostSpecificTypeMismatch              Code = "2200G"
	NullValueNotAllowed                   Code = "22004"
	NullValueNoIndicatorParameter         Code = "22002"
	NumericValueOutOfRange                Code = "22003"
	SequenceGeneratorLimitExceeded        Code = "2200H"
	StringDataLengthMismatch              Code = "22026"
	StringDataRightTruncation             Code = "22001"
	Substring                             Code = "22011"
	Trim                                  Code = "22027"
	UnterminatedCString                   Code = "22024"
	ZeroLengthCharacterString             Code = "2200F"
	FloatingPointException                Code = "22P01"
	InvalidTextRepresentation             Code = "22P02"
	InvalidBinaryRepresentation           Code = "22P03"
	BadCopyFileFormat                     Code = "22P04"
	UntranslatableCharacter               Code = "22P05"
	NotAnXMLDocument                      Code = "2200L"
	InvalidXMLDocument                    Code = "2200M"
	InvalidXMLContent                     Code = "2200N"
	InvalidXMLComment                     Code = "2200S"
	InvalidXMLProcessingInstruction       Code = "2200T"
	// Section: Class 23 - Integrity Constraint Violation
	IntegrityConstraintViolation Code = "23000"
	RestrictViolation            Code = "23001"
	NotNullViolation             Code = "23502"
	ForeignKeyViolation          Code = "23503"
	UniqueViolation              Code = "23505"
	CheckViolation               Code = "23514"
	ExclusionViolation           Code = "23P01"
	// Section: Class 24 - Invalid Cursor State
	InvalidCursorState Code = "24000"
	// Section: Class 25 - Invalid Transaction State
	InvalidTransactionState                         Code = "25000"
	ActiveSQLTransaction                            Code = "25001"
	BranchTransactionAlreadyActive                  Code = "25002"
	HeldCursorRequiresSameIsolationLevel            Code = "25008"
	InappropriateAccessModeForBranchTransaction     Code = "25003"
	InappropriateIsolationLevelForBranchTransaction Code = "25004"
	NoActiveSQLTransactionForBranchTransaction      Code = "25005"
	ReadOnlySQLTransaction                          Code = "25006"
	SchemaAndDataStatementMixingNotSupported        Code = "25007"
	NoActiveSQLTransaction                          Code = "25P01"
	InFailedSQLTransaction                          Code = "25P02"
	// Section: Class 26 - Invalid SQL Statement Name
	InvalidSQLStatementName Code = "26000"
	// Section: Class 27 - Triggered Data Change Violation
	TriggeredDataChangeViolation Code = "27000"
	// Section: Class 28 - Invalid Authorization Specification
	InvalidAuthorizationSpecification Code = "28000"
	InvalidPassword                   Code = "28P01"
	// Section: Class 2B - Dependent Privilege Descriptors Still Exist
	DependentPrivilegeDescriptorsStillExist Code = "2B000"
	DependentObjectsStillExist              Code = "2BP01"
	// Section: Class 2D - Invalid Transaction Termination
	InvalidTransactionTermination Code = "2D000"
	// Section: Class 2F - SQL Routine Exception
	RoutineExceptionFunctionExecutedNoReturnStatement Code = "2F005"
	RoutineExceptionModifyingSQLDataNotPermitted      Code = "2F002"
	RoutineExceptionProhibitedSQLStatementAttempted   Code = "2F003"
	RoutineExceptionReadingSQLDataNotPermitted        Code = "2F004"
	// Section: Class 34 - Invalid Cursor Name
	InvalidCursorName Code = "34000"
	// Section: Class 38 - External Routine Exception
	ExternalRoutineException                       Code = "38000"
	ExternalRoutineContainingSQLNotPermitted       Code = "38001"
	ExternalRoutineModifyingSQLDataNotPermitted    Code = "38002"
	ExternalRoutineProhibitedSQLStatementAttempted Code = "38003"
	ExternalRoutineReadingSQLDataNotPermitted      Code = "38004"
	// Section: Class 39 - External Routine Invocation Exception
	ExternalRoutineInvocationException     Code = "39000"
	ExternalRoutineInvalidSQLstateReturned Code = "39001"
	ExternalRoutineNullValueNotAllowed     Code = "39004"
	ExternalRoutineTriggerProtocolViolated Code = "39P01"
	ExternalRoutineSrfProtocolViolated     Code = "39P02"
	// Section: Class 3B - Savepoint Exception
	SavepointException            Code = "3B000"
	InvalidSavepointSpecification Code = "3B001"
	// Section: Class 3D - Invalid Catalog Name
	InvalidCatalogName Code = "3D000"
	// Section: Class 3F - Invalid Schema Name
	InvalidSchemaName Code = "3F000"
	// Section: Class 40 - Transaction Rollback
	TransactionRollback                     Code = "40000"
	TransactionIntegrityConstraintViolation Code = "40002"
	SerializationFailure                    Code = "40001"
	StatementCompletionUnknown              Code = "40003"
	DeadlockDetected                        Code = "40P01"
	// Section: Class 42 - Syntax Error or Access Rule Violation
	SyntaxErrorOrAccessRuleViolation   Code = "42000"
	Syntax                             Code = "42601"
	InsufficientPrivilege              Code = "42501"
	CannotCoerce                       Code = "42846"
	Grouping                           Code = "42803"
	Windowing                          Code = "42P20"
	InvalidRecursion                   Code = "42P19"
	InvalidForeignKey                  Code = "42830"
	InvalidName                        Code = "42602"
	NameTooLong                        Code = "42622"
	ReservedName                       Code = "42939"
	DatatypeMismatch                   Code = "42804"
	IndeterminateDatatype              Code = "42P18"
	CollationMismatch                  Code = "42P21"
	IndeterminateCollation             Code = "42P22"
	WrongObjectType                    Code = "42809"
	UndefinedColumn                    Code = "42703"
	UndefinedCursor                    Code = "34000"
	UndefinedDatabase                  Code = "3D000"
	UndefinedFunction                  Code = "42883"
	UndefinedPreparedStatement         Code = "26000"
	UndefinedSchema                    Code = "3F000"
	UndefinedTable                     Code = "42P01"
	UndefinedParameter                 Code = "42P02"
	UndefinedObject                    Code = "42704"
	DuplicateColumn                    Code = "42701"
	DuplicateCursor                    Code = "42P03"
	DuplicateDatabase                  Code = "42P04"
	DuplicateFunction                  Code = "42723"
	DuplicatePreparedStatement         Code = "42P05"
	DuplicateSchema                    Code = "42P06"
	DuplicateRelation                  Code = "42P07"
	DuplicateAlias                     Code = "42712"
	DuplicateObject                    Code = "42710"
	AmbiguousColumn                    Code = "42702"
	AmbiguousFunction                  Code = "42725"
	AmbiguousParameter                 Code = "42P08"
	AmbiguousAlias                     Code = "42P09"
	InvalidColumnReference             Code = "42P10"
	InvalidColumnDefinition            Code = "42611"
	InvalidCursorDefinition            Code = "42P11"
	InvalidDatabaseDefinition          Code = "42P12"
	InvalidFunctionDefinition          Code = "42P13"
	InvalidPreparedStatementDefinition Code = "42P14"
	InvalidSchemaDefinition            Code = "42P15"
	InvalidTableDefinition             Code = "42P16"
	InvalidObjectDefinition            Code = "42P17"
	FileAlreadyExists                  Code = "42C01"
	// Section: Class 44 - WITH CHECK OPTION Violation
	WithCheckOptionViolation Code = "44000"
	// Section: Class 53 - Insufficient Resources
	InsufficientResources      Code = "53000"
	DiskFull                   Code = "53100"
	OutOfMemory                Code = "53200"
	TooManyConnections         Code = "53300"
	ConfigurationLimitExceeded Code = "53400"
	// Section: Class 54 - Program Limit Exceeded
	ProgramLimitExceeded Code = "54000"
	StatementTooComplex  Code = "54001"
	TooManyColumns       Code = "54011"
	TooManyArguments     Code = "54023"
	// Section: Class 55 - Object Not In Prerequisite State
	ObjectNotInPrerequisiteState Code = "55000"
	ObjectInUse                  Code = "55006"
	CantChangeRuntimeParam       Code = "55P02"
	LockNotAvailable             Code = "55P03"
	// Section: Class 57 - Operator Intervention
	OperatorIntervention Code = "57000"
	QueryCanceled        Code = "57014"
	AdminShutdown        Code = "57P01"
	CrashShutdown        Code = "57P02"
	CannotConnectNow     Code = "57P03"
	DatabaseDropped      Code = "57P04"
	// Section: Class 58 - System Error
	System        Code = "58000"
	Io            Code = "58030"
	UndefinedFile Code = "58P01"
	DuplicateFile Code = "58P02"
	// Section: Class F0 - Configuration File Error
	ConfigFile     Code = "F0000"
	LockFileExists Code = "F0001"
	// Section: Class HV - Foreign Data Wrapper Error (SQL/MED)
	FdwError                             Code = "HV000"
	FdwColumnNameNotFound                Code = "HV005"
	FdwDynamicParameterValueNeeded       Code = "HV002"
	FdwFunctionSequenceError             Code = "HV010"
	FdwInconsistentDescriptorInformation Code = "HV021"
	FdwInvalidAttributeValue             Code = "HV024"
	FdwInvalidColumnName                 Code = "HV007"
	FdwInvalidColumnNumber               Code = "HV008"
	FdwInvalidDataType                   Code = "HV004"
	FdwInvalidDataTypeDescriptors        Code = "HV006"
	FdwInvalidDescriptorFieldIdentifier  Code = "HV091"
	FdwInvalidHandle                     Code = "HV00B"
	FdwInvalidOptionIndex                Code = "HV00C"
	FdwInvalidOptionName                 Code = "HV00D"
	FdwInvalidStringLengthOrBufferLength Code = "HV090"
	FdwInvalidStringFormat               Code = "HV00A"
	FdwInvalidUseOfNullPointer           Code = "HV009"
	FdwTooManyHandles                    Code = "HV014"
	FdwOutOfMemory                       Code = "HV001"
	FdwNoSchemas                         Code = "HV00P"
	FdwOptionNameNotFound                Code = "HV00J"
	FdwReplyHandle                       Code = "HV00K"
	FdwSchemaNotFound                    Code = "HV00Q"
	FdwTableNotFound                     Code = "HV00R"
	FdwUnableToCreateExecution           Code = "HV00L"
	FdwUnableToCreateReply               Code = "HV00M"
	FdwUnableToEstablishConnection       Code = "HV00N"
	// Section: Class P0 - PL/pgSQL Error
	PLpgSQL        Code = "P0000"
	RaiseException Code = "P0001"
	NoDataFound    Code = "P0002"
	TooManyRows    Code = "P0003"
	AssertFailure  Code = "P0004"
	// Section: Class XX - Internal Error
	Internal       Code = "XX000"
	DataCorrupted  Code = "XX001"
	IndexCorrupted Code = "XX002"
)

// The following errors are CockroachDB-specific.

var (
	// Uncategorized is used for errors that flow out to a client
	// when there's no code known yet.
	Uncategorized Code = "XXUUU"

	// CCLRequired signals that a CCL binary is required to complete this
	// task.
	CCLRequired Code = "XXC01"

	// CCLValidLicenseRequired signals that a valid CCL license is
	// required to complete this task.
	CCLValidLicenseRequired Code = "XXC02"

	// TransactionCommittedWithSchemaChangeFailure signals that the
	// non-DDL payload of a transaction was committed successfully but
	// some DDL operation failed, without rolling back the rest of the
	// transaction.
	//
	// We define a separate code instead of reusing a code from
	// PostgreSQL (like StatementCompletionUnknown) because that makes
	// it easier to document the error (this code only occurs in this
	// particular situation) in a way that's unique to CockroachDB.
	//
	// We also use a "XX" code for this for several reasons:
	// - it needs to override any other pg code set "underneath" in the cause.
	// - it forces implementers of logic tests to be mindful about
	//   this situation. The logic test runner will remind the implementer
	//   that:
	//       serious error with code "XXA00" occurred; if expected,
	//       must use 'error pgcode XXA00 ...'
	TransactionCommittedWithSchemaChangeFailure Code = "XXA00"

	// Class 22C - Semantic errors in the structure of a SQL statement.

	// ScalarOperationCannotRunWithoutFullSessionContext signals that an
	// operator or built-in function was used that requires a full session
	// context and thus cannot be run in a background job or away from the SQL
	// gateway.
	ScalarOperationCannotRunWithoutFullSessionContext Code = "22C01"

	// Class 55C - Object Not In Prerequisite State (Cockroach extension)

	// SchemaChangeOccurred signals that a DDL change to the targets of a
	// CHANGEFEED has lead to its termination. If this error code is received
	// the CHANGEFEED will have previously emitted a resolved timestamp which
	// precedes the hlc timestamp of the relevant DDL transaction.
	SchemaChangeOccurred Code = "55C01"

	// NoPrimaryKey signals that a table descriptor is invalid because the table
	// does not have a primary key.
	NoPrimaryKey Code = "55C02"

	// Class 58C - System errors related to CockroachDB node problems.

	// RangeUnavailable signals that some data from the cluster cannot be
	// accessed (e.g. because all replicas awol).
	RangeUnavailable Code = "58C00"
	// DeprecatedRangeUnavailable is code that we used for RangeUnavailable until 19.2.
	// 20.1 needs to recognize it coming from 19.2 nodes.
	DeprecatedRangeUnavailable Code = "XXC00"

	// InternalConnectionFailure refers to a networking error encountered
	// internally on a connection between different Cockroach nodes.
	InternalConnectionFailure Code = "58C01"
	// DeprecatedInternalConnectionFailure is code that we used for
	// InternalConnectionFailure until 19.2.
	// 20.1 needs to recognize it coming from 19.2 nodes.
	DeprecatedInternalConnectionFailure Code = ConnectionFailure
)
