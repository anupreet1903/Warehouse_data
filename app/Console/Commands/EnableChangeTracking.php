<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Exception;

class EnableChangeTracking extends Command
{
    protected $signature = 'db:enable-change-tracking';
    protected $description = 'Enable change tracking for all tables in the database';

    public function handle()
    {
        try {
            // Enable change tracking on the database level
            DB::statement('ALTER DATABASE ' . DB::getDatabaseName() . ' SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)');
            $this->info('Change tracking enabled for the database.');

            // Get all tables
            $tables = DB::select("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_catalog = ?", [DB::getDatabaseName()]);

            foreach ($tables as $table) {
                $tableName = $table->table_name;
                
                // Check if change tracking is already enabled for this table
                $changeTrackingEnabled = DB::select("SELECT object_id FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID(?)", [$tableName]);
                
                if (empty($changeTrackingEnabled)) {
                    // Enable change tracking for the table
                    DB::statement("ALTER TABLE $tableName ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)");
                    $this->info("Change tracking enabled for table: $tableName");
                } else {
                    $this->info("Change tracking already enabled for table: $tableName");
                }
            }

            $this->info('Change tracking has been enabled for all tables.');
        } catch (Exception $e) {
            $this->error('An error occurred: ' . $e->getMessage());
        }
    }
}