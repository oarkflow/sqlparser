SELECT *
FROM orders
WHERE account_id = :account_id
  AND status = @status
  AND created_at >= $1
  AND total >= ?;

SELECT make_interval(days => :days, hours := ?)
FROM system_settings
WHERE tenant_id = :tenant_id;

CALL refresh_account_cache(account_id => :account_id, force := ?);
