package snowflake

import (
	"fmt"
	"strings"
)

// The ACCOUNT_USAGE discovery path.
//
// Every SHOW command the connector issues returns only the objects the session role holds
// at least one privilege on, so full-account visibility through SHOW requires either
// per-object OWNERSHIP or the account-level MANAGE GRANTS privilege. MANAGE GRANTS is the
// ability to grant or revoke any privilege on any object as if the invoking role owned it -
// including granting further privileges to itself - which is an unbounded standing
// privilege that a regulated tenant's security review will not sign off on.
//
// SNOWFLAKE.ACCOUNT_USAGE is the read-only alternative. Its views are account-wide by
// construction and are gated by two database roles rather than by per-object grants:
//
//	GRANT DATABASE ROLE SNOWFLAKE.SECURITY_VIEWER TO ROLE <connector role>;  -- USERS, ROLES, GRANTS_TO_*
//	GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER   TO ROLE <connector role>;  -- DATABASES, SCHEMATA, TABLES
//
// The trade-offs:
//
//   - Latency. ACCOUNT_USAGE views lag the live account by 90 minutes to 3 hours depending
//     on the view, so a sync reflects a slightly stale account. The SHOW path is live.
//   - A warehouse. These are SELECTs, so the service account needs USAGE on a warehouse and
//     the warehouse must be able to resume. SHOW commands are metadata-only and need none.
//
// Both trade-offs are documented by Snowflake and are accepted as the cost of dropping the
// MANAGE GRANTS requirement.
//
// Implementation note: rather than a parallel set of response types and parsers, each
// statement below aliases its ACCOUNT_USAGE columns to the exact column names the
// corresponding SHOW command returns, and casts each one to the type the shared
// ResultSetMetadata.ParseRow expects (TEXT for strings and booleans, TIMESTAMP_LTZ for
// timestamps - the SQL API reports a SELECT's real column types, and ParseRow rejects
// anything else). The existing GetUsers/GetAccountRoles/GetDatabases/ListSchemas/
// ListTables/GetTableGrants/GetAccountRoleGrantees parsers then work unchanged, so the two
// discovery paths cannot drift apart in how a row becomes a resource.
//
// Deliberately NOT on this path (there is no ACCOUNT_USAGE equivalent, so these keep using
// SHOW/DESCRIBE regardless of discovery mode): integrations, per-user RSA public key
// last-set timestamps, programmatic access tokens, and secrets.

const (
	accountUsageUsersView         = "SNOWFLAKE.ACCOUNT_USAGE.USERS"
	accountUsageRolesView         = "SNOWFLAKE.ACCOUNT_USAGE.ROLES"
	accountUsageGrantsToUsersView = "SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS"
	accountUsageGrantsToRolesView = "SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES"
	accountUsageDatabasesView     = "SNOWFLAKE.ACCOUNT_USAGE.DATABASES"
	accountUsageSchemataView      = "SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA"
	accountUsageTablesView        = "SNOWFLAKE.ACCOUNT_USAGE.TABLES"
	// accountUsageRoleGrantsViews names both views the role-grantee union reads, for a
	// diagnostic that points at the pair rather than at an arbitrary half of it.
	accountUsageRoleGrantsViews = "SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS and SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES"
)

// accountUsageUserColumns aliases ACCOUNT_USAGE.USERS onto the SHOW USERS column names in
// userStructFieldToColumnMap. Unlike SHOW USERS these columns are never blanked per user:
// the view has no per-user privilege requirement, which is exactly why this path also fixes
// the partial-visibility problem rather than only the MANAGE GRANTS one.
const accountUsageUserColumns = `NAME::TEXT AS "name", ` +
	`LOGIN_NAME::TEXT AS "login_name", ` +
	`DISPLAY_NAME::TEXT AS "display_name", ` +
	`FIRST_NAME::TEXT AS "first_name", ` +
	`LAST_NAME::TEXT AS "last_name", ` +
	`EMAIL::TEXT AS "email", ` +
	`DISABLED::TEXT AS "disabled", ` +
	`SNOWFLAKE_LOCK::TEXT AS "snowflake_lock", ` +
	`DEFAULT_ROLE::TEXT AS "default_role", ` +
	`HAS_RSA_PUBLIC_KEY::TEXT AS "has_rsa_public_key", ` +
	`HAS_PASSWORD::TEXT AS "has_password", ` +
	`LAST_SUCCESS_LOGIN::TIMESTAMP_LTZ AS "last_success_login", ` +
	`TYPE::TEXT AS "type", ` +
	`HAS_MFA::TEXT AS "has_mfa", ` +
	`COMMENT::TEXT AS "comment"`

// accountUsageDatabaseColumns aliases ACCOUNT_USAGE.DATABASES onto the SHOW DATABASES
// columns in databaseStructFieldToColumnMap.
//
// "origin" is a literal empty string: the view has no equivalent column. Database.
// IsSharedOrSystem treats a non-empty origin as shared, but it also treats the
// IMPORTED DATABASE / APPLICATION values of this view's TYPE column (mapped to "kind") and
// a SNOWFLAKE owner as shared, so the classification is preserved without it.
const accountUsageDatabaseColumns = `DATABASE_NAME::TEXT AS "name", ` +
	`DATABASE_OWNER::TEXT AS "owner", ` +
	`TYPE::TEXT AS "kind", ` +
	`''::TEXT AS "origin"`

// accountUsageSchemaColumns aliases ACCOUNT_USAGE.SCHEMATA onto the SHOW SCHEMAS columns in
// schemaStructFieldToColumnMap.
const accountUsageSchemaColumns = `SCHEMA_NAME::TEXT AS "name", ` +
	`CATALOG_NAME::TEXT AS "database_name"`

// accountUsageTableColumns aliases ACCOUNT_USAGE.TABLES onto the SHOW TABLES columns in
// tableStructFieldToColumnMap. "kind" is the literal TABLE rather than the view's
// TABLE_TYPE: SHOW TABLES reports the kind of a table, never VIEW, and the value feeds
// straight back into the grants lookup's object kind.
const accountUsageTableColumns = `CREATED::TIMESTAMP_LTZ AS "created_on", ` +
	`TABLE_NAME::TEXT AS "name", ` +
	`TABLE_SCHEMA::TEXT AS "schema_name", ` +
	`TABLE_CATALOG::TEXT AS "database_name", ` +
	`'TABLE'::TEXT AS "kind", ` +
	`COMMENT::TEXT AS "comment", ` +
	`TABLE_OWNER::TEXT AS "owner"`

// accountUsageTableGrantColumns aliases ACCOUNT_USAGE.GRANTS_TO_ROLES onto the
// SHOW GRANTS ON TABLE/VIEW columns in tableGrantStructFieldToColumnMap.
const accountUsageTableGrantColumns = `CREATED_ON::TIMESTAMP_LTZ AS "created_on", ` +
	`PRIVILEGE::TEXT AS "privilege", ` +
	`GRANTED_ON::TEXT AS "granted_on", ` +
	`NAME::TEXT AS "name", ` +
	`GRANTED_TO::TEXT AS "granted_to", ` +
	`GRANTEE_NAME::TEXT AS "grantee_name", ` +
	`GRANT_OPTION::TEXT AS "grant_option", ` +
	`GRANTED_BY::TEXT AS "granted_by"`

// accountUsageNonViewTableTypes are the ACCOUNT_USAGE.TABLES TABLE_TYPE values excluded to
// match SHOW TABLES, which lists neither views nor external tables (SHOW VIEWS and SHOW
// EXTERNAL TABLES cover those, and the connector issues neither).
//
// TEMPORARY TABLE is excluded for a different reason: SHOW TABLES only ever returns the
// calling session's own temporary tables, so the connector never sees one on that path,
// while the view retains every session's until they are tombstoned. Without this the
// ACCOUNT_USAGE path would emit table resources for other sessions' temp tables that no
// longer exist and then delete them on the following sync.
//
// This stays a denylist rather than an allowlist on purpose: an allowlist would silently
// drop any TABLE_TYPE Snowflake adds later, and a dropped resource reads as a deletion.
const accountUsageNonViewTableTypes = `'VIEW', 'MATERIALIZED VIEW', 'EXTERNAL TABLE', 'TEMPORARY TABLE'`

// accountUsageKeysetPredicate renders the keyset-pagination predicate for an ACCOUNT_USAGE
// list query. The SHOW path paginates with "LIMIT n FROM '<last name>'", which is keyset
// pagination on the object name; this reproduces it exactly so the cursor the connector
// stores means the same thing in both modes and a sync can't skip or repeat a page after a
// mode change mid-sync.
func accountUsageKeysetPredicate(column, cursor string) string {
	if cursor == "" {
		return ""
	}
	return fmt.Sprintf(" AND %s > '%s'", column, escapeStringLiteral(cursor))
}

// accountUsageLimitClause renders a LIMIT clause, omitting it for a non-positive limit so a
// caller that does not paginate gets the whole result set.
//
// The SHOW branches do NOT match this for a non-positive limit: they emit "LIMIT 0", which
// Snowflake honours literally by returning no rows. The asymmetry is deliberate rather than
// an oversight - SHOW's keyset "FROM '<name>'" is a sub-clause of LIMIT, so omitting LIMIT
// while paginating is a SQL compilation error and there is no unbounded keyset form to match.
// No caller passes a non-positive limit today (every list path passes a positive page size);
// anything that starts to must decide what unbounded means on both paths at once.
func accountUsageLimitClause(limit int) string {
	if limit <= 0 {
		return ""
	}
	return fmt.Sprintf(" LIMIT %d", limit)
}

// accountUsageListUsersStatement is the ACCOUNT_USAGE equivalent of SHOW USERS.
//
// DELETED_ON IS NULL is required on every ACCOUNT_USAGE object view: these views retain
// dropped objects as tombstone rows, and without the filter a dropped user would keep
// syncing as a live account indefinitely.
func accountUsageListUsersStatement(cursor string, limit int) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE DELETED_ON IS NULL%s ORDER BY NAME%s;",
		accountUsageUserColumns, accountUsageUsersView,
		accountUsageKeysetPredicate("NAME", cursor), accountUsageLimitClause(limit),
	)
}

// accountUsageGetUserStatement is the ACCOUNT_USAGE equivalent of DESCRIBE USER. It returns
// the same row shape as accountUsageListUsersStatement (one row per user) rather than
// DESCRIBE USER's property/value pairs, so GetUser parses it with GetUsers and not GetUser.
func accountUsageGetUserStatement(username string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE DELETED_ON IS NULL AND NAME = '%s' ORDER BY NAME LIMIT 1;",
		accountUsageUserColumns, accountUsageUsersView, escapeStringLiteral(username),
	)
}

// accountUsageRoleTypePredicate restricts ACCOUNT_USAGE.ROLES to account roles.
//
// The view is a superset of SHOW ROLES: it also carries database roles, instance roles and
// application roles, distinguished by ROLE_TYPE. SHOW ROLES lists account roles only (SHOW
// DATABASE ROLES IN DATABASE <db> is the separate command, which this connector never
// issues). Without this filter, ACCOUNT_USAGE discovery would sync every database role in
// the account as an account_role resource with a grantable entitlement - and database-role
// names are only unique within their database, so two databases each holding an ANALYST
// role would collide on a single account_role::ANALYST id.
const accountUsageRoleTypePredicate = " AND ROLE_TYPE = 'ROLE'"

// accountUsageListRolesStatement is the ACCOUNT_USAGE equivalent of SHOW ROLES.
func accountUsageListRolesStatement(cursor string, limit int) string {
	return fmt.Sprintf(
		"SELECT NAME::TEXT AS \"name\" FROM %s WHERE DELETED_ON IS NULL%s%s ORDER BY NAME%s;",
		accountUsageRolesView, accountUsageRoleTypePredicate,
		accountUsageKeysetPredicate("NAME", cursor), accountUsageLimitClause(limit),
	)
}

// accountUsageGetRoleStatement is the ACCOUNT_USAGE equivalent of SHOW ROLES LIKE. It is an
// exact-match lookup, so unlike the SHOW path it has no LIKE wildcard hazard: _ and % in a
// role name stay literal and cannot let a colliding role crowd out the real one.
func accountUsageGetRoleStatement(roleName string) string {
	return fmt.Sprintf(
		"SELECT NAME::TEXT AS \"name\" FROM %s WHERE DELETED_ON IS NULL%s AND NAME = '%s' ORDER BY NAME LIMIT 1;",
		accountUsageRolesView, accountUsageRoleTypePredicate, escapeStringLiteral(roleName),
	)
}

// accountUsageRoleGranteesStatement is the ACCOUNT_USAGE equivalent of SHOW GRANTS OF ROLE.
//
// SHOW GRANTS OF ROLE returns both the users and the roles a role has been granted to, and
// ACCOUNT_USAGE splits those across two views, so the two halves are unioned back together:
// GRANTS_TO_USERS for role-to-user grants, and the GRANTED_ON = 'ROLE' rows of
// GRANTS_TO_ROLES for role-to-role (and role-to-database-role) grants. Only the first
// branch of a UNION ALL needs the output aliases; Snowflake takes the set operation's column
// names from it, which is also what the trailing ORDER BY refers to.
func accountUsageRoleGranteesStatement(roleName string) string {
	escaped := escapeStringLiteral(roleName)
	return fmt.Sprintf(
		"SELECT ROLE::TEXT AS \"role\", 'USER'::TEXT AS \"granted_to\", GRANTEE_NAME::TEXT AS \"grantee_name\" "+
			"FROM %s WHERE DELETED_ON IS NULL AND ROLE = '%s' "+
			"UNION ALL "+
			"SELECT NAME::TEXT, GRANTED_TO::TEXT, GRANTEE_NAME::TEXT "+
			"FROM %s WHERE DELETED_ON IS NULL AND GRANTED_ON = 'ROLE' AND PRIVILEGE = 'USAGE' AND NAME = '%s' "+
			"ORDER BY \"granted_to\", \"grantee_name\";",
		accountUsageGrantsToUsersView, escaped, accountUsageGrantsToRolesView, escaped,
	)
}

// accountUsageListDatabasesStatement is the ACCOUNT_USAGE equivalent of SHOW DATABASES.
// The object views spell the tombstone column DELETED, not DELETED_ON as the security views do.
func accountUsageListDatabasesStatement(cursor string, limit int) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE DELETED IS NULL%s ORDER BY DATABASE_NAME%s;",
		accountUsageDatabaseColumns, accountUsageDatabasesView,
		accountUsageKeysetPredicate("DATABASE_NAME", cursor), accountUsageLimitClause(limit),
	)
}

// accountUsageGetDatabaseStatement is the ACCOUNT_USAGE equivalent of SHOW DATABASES LIKE.
func accountUsageGetDatabaseStatement(name string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE DELETED IS NULL AND DATABASE_NAME = '%s' ORDER BY DATABASE_NAME LIMIT 1;",
		accountUsageDatabaseColumns, accountUsageDatabasesView, escapeStringLiteral(name),
	)
}

// accountUsageListSchemasStatement is the ACCOUNT_USAGE equivalent of
// SHOW SCHEMAS IN DATABASE.
func accountUsageListSchemasStatement(databaseName string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE DELETED IS NULL AND CATALOG_NAME = '%s' ORDER BY SCHEMA_NAME;",
		accountUsageSchemaColumns, accountUsageSchemataView, escapeStringLiteral(databaseName),
	)
}

// accountUsageListTablesStatement is the ACCOUNT_USAGE equivalent of
// SHOW TABLES IN SCHEMA.
func accountUsageListTablesStatement(databaseName, schemaName, cursor string, limit int) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE DELETED IS NULL AND TABLE_CATALOG = '%s' AND TABLE_SCHEMA = '%s' "+
			"AND TABLE_TYPE NOT IN (%s)%s ORDER BY TABLE_NAME%s;",
		accountUsageTableColumns, accountUsageTablesView,
		escapeStringLiteral(databaseName), escapeStringLiteral(schemaName),
		accountUsageNonViewTableTypes,
		accountUsageKeysetPredicate("TABLE_NAME", cursor), accountUsageLimitClause(limit),
	)
}

// accountUsageGetTableStatement is the ACCOUNT_USAGE equivalent of SHOW TABLES LIKE ... IN SCHEMA.
func accountUsageGetTableStatement(databaseName, schemaName, tableName string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE DELETED IS NULL AND TABLE_CATALOG = '%s' AND TABLE_SCHEMA = '%s' "+
			"AND TABLE_NAME = '%s' AND TABLE_TYPE NOT IN (%s) ORDER BY TABLE_NAME LIMIT 1;",
		accountUsageTableColumns, accountUsageTablesView,
		escapeStringLiteral(databaseName), escapeStringLiteral(schemaName),
		escapeStringLiteral(tableName), accountUsageNonViewTableTypes,
	)
}

// accountUsageTableGrantsStatement is the ACCOUNT_USAGE equivalent of
// SHOW GRANTS ON TABLE / ON VIEW.
//
// Known gap: GRANTS_TO_ROLES records grants to roles and database roles only, so a privilege
// granted to a share (granted_to = 'SHARE' in SHOW GRANTS output) is not represented. The
// connector already ignores those rows - table entitlements are built only from ROLE and
// USER grantees - so the synced result is unchanged.
func accountUsageTableGrantsStatement(databaseName, schemaName, tableName, objectKind string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE DELETED_ON IS NULL AND GRANTED_ON = '%s' "+
			"AND TABLE_CATALOG = '%s' AND TABLE_SCHEMA = '%s' AND NAME = '%s' "+
			"ORDER BY \"grantee_name\", \"privilege\";",
		accountUsageTableGrantColumns, accountUsageGrantsToRolesView,
		normalizeObjectKind(objectKind),
		escapeStringLiteral(databaseName), escapeStringLiteral(schemaName),
		escapeStringLiteral(tableName),
	)
}

// normalizeObjectKind maps a table resource's profile "kind" to the GRANTED_ON value
// ACCOUNT_USAGE records for it, defaulting to TABLE. It mirrors how the SHOW path picks
// between SHOW GRANTS ON TABLE and ON VIEW from the same profile value.
func normalizeObjectKind(objectKind string) string {
	if strings.EqualFold(strings.TrimSpace(objectKind), "VIEW") {
		return "VIEW"
	}
	return "TABLE"
}
