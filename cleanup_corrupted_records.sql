-- ═══════════════════════════════════════════════════════════════════════════
-- BHOOMI DATABASE CLEANUP SCRIPT
-- Purpose: Remove records corrupted by HTML form data extraction bug
-- Date: December 16, 2025
-- ═══════════════════════════════════════════════════════════════════════════

-- IMPORTANT: This script will DELETE data. Make a backup first!
-- To backup: .backup /Users/sks/Documents/POWER-BHOOMI/bhoomi_data_backup.db

.headers on
.mode column

-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 1: IDENTIFY CORRUPTED RECORDS
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' as '';
SELECT 'STEP 1: IDENTIFYING CORRUPTED RECORDS' as '';
SELECT '═══════════════════════════════════════════════════════════════' as '';
SELECT '' as '';

-- Count records with form text
SELECT 
    COUNT(*) as total_corrupted_records,
    COUNT(DISTINCT session_id) as affected_sessions,
    MIN(created_at) as earliest_corruption,
    MAX(created_at) as latest_corruption
FROM land_records 
WHERE owner_name LIKE '%Select%'
   OR owner_name LIKE '%District%'
   OR owner_name LIKE '%Toggle%'
   OR owner_name LIKE '%Survey Number%'
   OR owner_name LIKE '%BAGALKOTE%'
   OR owner_name LIKE '%BALLARI%'
   OR owner_name LIKE '%BANGALORE%'
   OR LENGTH(owner_name) > 200;

SELECT '' as '';

-- Show sample corrupted records
SELECT '─────────────────────────────────────────────────────────────────' as '';
SELECT 'SAMPLE CORRUPTED RECORDS (first 5):' as '';
SELECT '─────────────────────────────────────────────────────────────────' as '';
SELECT '' as '';

SELECT 
    id,
    village,
    survey_no,
    hissa,
    SUBSTR(owner_name, 1, 80) as owner_name_preview,
    created_at
FROM land_records 
WHERE owner_name LIKE '%Select%'
   OR owner_name LIKE '%District%'
   OR LENGTH(owner_name) > 200
LIMIT 5;

SELECT '' as '';

-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 2: IDENTIFY AFFECTED SESSIONS
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' as '';
SELECT 'STEP 2: AFFECTED SEARCH SESSIONS' as '';
SELECT '═══════════════════════════════════════════════════════════════' as '';
SELECT '' as '';

SELECT 
    session_id,
    COUNT(*) as corrupted_records,
    MIN(created_at) as first_record,
    MAX(created_at) as last_record
FROM land_records 
WHERE owner_name LIKE '%Select%'
   OR owner_name LIKE '%District%'
   OR LENGTH(owner_name) > 200
GROUP BY session_id
ORDER BY corrupted_records DESC;

SELECT '' as '';

-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 3: BACKUP REMINDER
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '═══════════════════════════════════════════════════════════════' as '';
SELECT '⚠️  IMPORTANT: MAKE A BACKUP BEFORE PROCEEDING!' as '';
SELECT '═══════════════════════════════════════════════════════════════' as '';
SELECT '' as '';
SELECT 'To create a backup, run this in sqlite3:' as '';
SELECT '.backup /Users/sks/Documents/POWER-BHOOMI/bhoomi_data_backup.db' as '';
SELECT '' as '';
SELECT 'Then run the DELETE commands below...' as '';
SELECT '' as '';

-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 4: DELETE CORRUPTED RECORDS (COMMENTED OUT FOR SAFETY)
-- ═══════════════════════════════════════════════════════════════════════════

-- Uncomment the following lines to actually delete the corrupted records:

-- BEGIN TRANSACTION;

-- -- Delete records with obvious form text
-- DELETE FROM land_records 
-- WHERE owner_name LIKE '%Select%'
--    OR owner_name LIKE '%District%'
--    OR owner_name LIKE '%Toggle%'
--    OR owner_name LIKE '%Survey Number%'
--    OR owner_name LIKE '%BAGALKOTE%'
--    OR owner_name LIKE '%BALLARI%'
--    OR owner_name LIKE '%BANGALORE%'
--    OR owner_name LIKE '%Taluk%'
--    OR owner_name LIKE '%Hobli%'
--    OR owner_name LIKE '%Village%'
--    OR owner_name LIKE '%Period%'
--    OR owner_name LIKE '%Surnoc%'
--    OR owner_name LIKE '%Hissa%'
--    OR LENGTH(owner_name) > 200
--    OR LENGTH(owner_name) < 3;

-- SELECT 'Deleted ' || changes() || ' corrupted records' as result;

-- COMMIT;

-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 5: VERIFY CLEANUP (UNCOMMENT AFTER DELETING)
-- ═══════════════════════════════════════════════════════════════════════════

-- SELECT '═══════════════════════════════════════════════════════════════' as '';
-- SELECT 'STEP 5: VERIFICATION' as '';
-- SELECT '═══════════════════════════════════════════════════════════════' as '';
-- SELECT '' as '';

-- -- Should return 0 if cleanup was successful
-- SELECT COUNT(*) as remaining_corrupted_records
-- FROM land_records 
-- WHERE owner_name LIKE '%Select%'
--    OR owner_name LIKE '%District%'
--    OR LENGTH(owner_name) > 200;

-- SELECT '' as '';

-- -- Show total remaining records
-- SELECT COUNT(*) as total_remaining_records
-- FROM land_records;

-- SELECT '' as '';
-- SELECT '✅ Cleanup complete! Remaining records are clean.' as '';
-- SELECT '' as '';

-- ═══════════════════════════════════════════════════════════════════════════
-- ALTERNATIVE: DELETE ENTIRE AFFECTED SESSIONS
-- ═══════════════════════════════════════════════════════════════════════════

-- If you prefer to delete ALL records from affected sessions (safer approach):

-- BEGIN TRANSACTION;

-- -- Delete all records from sessions that contain any corrupted data
-- DELETE FROM land_records 
-- WHERE session_id IN (
--     SELECT DISTINCT session_id 
--     FROM land_records 
--     WHERE owner_name LIKE '%Select%'
--        OR owner_name LIKE '%District%'
--        OR LENGTH(owner_name) > 200
-- );

-- SELECT 'Deleted all records from ' || changes() || ' affected sessions' as result;

-- COMMIT;

-- ═══════════════════════════════════════════════════════════════════════════
-- USAGE INSTRUCTIONS
-- ═══════════════════════════════════════════════════════════════════════════

-- To run this script:
-- 
-- 1. First, just analyze (safe - no changes):
--    sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db < cleanup_corrupted_records.sql
--
-- 2. To actually delete, edit this file and uncomment the DELETE sections
--
-- 3. ALWAYS backup first:
--    sqlite3 ~/Documents/POWER-BHOOMI/bhoomi_data.db ".backup bhoomi_data_backup.db"
