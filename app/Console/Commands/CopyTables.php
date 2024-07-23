<?php

namespace App\Console\Commands;

use Carbon\Carbon;
use Exception;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use parallel\Runtime;

class CopyTables extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'tables:copy';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Copy tables from source to destination using tempdb';

    private $batchSize = 5; // Number of tables to process in parallel
    private $maxRetries = 3;

    /**
     * Execute the console command.
     *
     * @return int
     */

    public function handle()
    {
        // $connections = [
        //     'sqlsrv_source' => env('DB_DATABASE_SOURCE'),
        //     'sqlsrv_destination' => env('DB_DATABASE_DESTINATION')
        // ];

        $sourceConnection = 'sqlsrv_source';
        $destinationConnection = 'sqlsrv_destination';

        $job = DB::connection('sqlsrv')->table('jobs')->where('job_name', 'tables:copy')->first();

        if (!$job || !$job->isEnabled) {
            $this->info('The cron job is disabled or not found in the jobs list');
            return 0;
        }

        DB::connection('sqlsrv')->table('log_details')->insert([
            'info_type' => 'Job',
            'message' => "Job tables:copy started",
            'created_at' => Carbon::now(),
            'updated_at' => Carbon::now(),
        ]);

        $this->cleanupTempDb();

        DB::connection($destinationConnection)->statement("EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'");

        $tables = DB::connection($sourceConnection)->select('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = \'BASE TABLE\'');
        $tableBatches = array_chunk($tables, $this->batchSize);

        foreach ($tableBatches as $batch) {
            $this->processBatch($batch, $job->job_id);
        }

        $this->info('Table copy process completed.');
    }


    private function cleanupTempDb()
    {
        $query = "
            DECLARE @sql NVARCHAR(MAX) = N'';
            SELECT @sql += N'DROP TABLE ' + QUOTENAME(name) + N';'
            FROM tempdb.sys.tables
            WHERE name LIKE '##%' AND OBJECT_ID('tempdb..' + name) IS NOT NULL;
            EXEC sp_executesql @sql;
        ";
        DB::connection('sqlsrv_tempdb')->unprepared($query);
        $this->info("Cleaned up leftover temporary tables in tempdb.");
    }
    private function processBatch($batch, $jobId)
    {
        if (function_exists('pcntl_fork')) {
            // Use forking for Unix-like systems
            $this->processBatchWithFork($batch, $jobId);
        } else {
            // Fallback to sequential processing for Windows
            $this->processBatchSequential($batch, $jobId);
        }
    }

    private function processBatchWithFork($batch, $jobId)
    {
        $pids = [];
        foreach ($batch as $table) {
            $pid = pcntl_fork();
            if ($pid == -1) {
                // Fork failed
                die('Could not fork');
            } else if ($pid) {
                // Parent process
                $pids[] = $pid;
            } else {
                // Child process
                $this->copyTable($table, $jobId);
                exit();
            }
        }

        // Wait for all child processes to finish
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }
    }

    private function processBatchSequential($batch, $jobId)
    {
        foreach ($batch as $table) {
            $this->copyTable($table, $jobId);
        }
    }

    private function copyTable($tableObj, $jobId)
    {
        $tableName = $tableObj->TABLE_NAME;
        $startTime = microtime(true);
        $error = null;
        $isSuccessful = false;

        try {
            DB::beginTransaction();

            $columns = $this->getColumns($tableName);
            $this->copyToTempDb($tableName, $columns);
            $this->copyFromTempDbToDestination($tableName, $columns);
            $this->dropTempTable($tableName);

            DB::commit();
            $isSuccessful = true;
        } catch (Exception $e) {
            DB::rollBack();
            $error = $e->getMessage();
            $this->retryOperation(function () use ($tableName, $columns) {
                $this->copyToTempDb($tableName, $columns);
                $this->copyFromTempDbToDestination($tableName, $columns);
                $this->dropTempTable($tableName);
            });
        }

        $executionTime = microtime(true) - $startTime;
        $this->logJobDetails($tableName, $jobId, $isSuccessful, $error, $executionTime);

        if ($error) {
            Log::error("Error processing table {$tableName}: {$error}");
        }
    }

    private function retryOperation($operation)
    {
        $retries = 0;
        while ($retries < $this->maxRetries) {
            try {
                $operation();
                return;
            } catch (Exception $e) {
                $retries++;
                if ($retries >= $this->maxRetries) {
                    throw $e;
                }
                sleep(pow(2, $retries)); // Exponential backoff
            }
        }
    }



    private function getColumns($tableName)
    {
        $columnsQuery = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
                      FROM INFORMATION_SCHEMA.COLUMNS
                      WHERE TABLE_NAME = ?
                      ORDER BY ORDINAL_POSITION";

        return DB::connection('sqlsrv_source')->select($columnsQuery, [$tableName]);
    }

    private function formatColumnDefinition($column)
    {
        $definition = "[{$column->COLUMN_NAME}] {$column->DATA_TYPE}";

        if (in_array($column->DATA_TYPE, ['char', 'varchar', 'nchar', 'nvarchar'])) {
            $length = $column->CHARACTER_MAXIMUM_LENGTH == -1 ? 'MAX' : $column->CHARACTER_MAXIMUM_LENGTH;
            $definition .= "($length)";
        } elseif (in_array($column->DATA_TYPE, ['decimal', 'numeric'])) {
            $definition .= "({$column->NUMERIC_PRECISION}, {$column->NUMERIC_SCALE})";
        }

        return $definition;
    }

    private function copyToTempDb($tableName, $columns)
    {
        $columnList = implode(', ', array_map(function ($col) {
            return "[{$col->COLUMN_NAME}]";
        }, $columns));

        $sourceTableName = "[{$tableName}]";
        $tempTableName = "##" . str_replace('-', '_', $tableName);

        $query = "SELECT $columnList INTO tempdb..$tempTableName FROM source_db.dbo.$sourceTableName";
        DB::connection('sqlsrv_source')->unprepared($query);
        $this->info("Copied $tableName to tempdb.");
    }

    private function copyFromTempDbToDestination($tableName, $columns)
    {
        $columnDefinitions = implode(', ', array_map([$this, 'formatColumnDefinition'], $columns));
        $columnList = implode(', ', array_map(function ($col) {
            return "[{$col->COLUMN_NAME}]";
        }, $columns));

        $destinationTableName = "[{$tableName}]";
        $tempTableName = "##" . str_replace('-', '_', $tableName);

        // Add warehouse_isDeleted and latest_updated columns
        $additionalColumns = "
        , [warehouse_isDeleted] BIT NOT NULL DEFAULT 0
        , [latest_updated] DATETIME2 NULL
    ";

        $query = "
    IF OBJECT_ID('destination_db.dbo.$destinationTableName', 'U') IS NOT NULL 
        DROP TABLE destination_db.dbo.$destinationTableName;
    
    CREATE TABLE destination_db.dbo.$destinationTableName (
        $columnDefinitions
        $additionalColumns
    );
    
    INSERT INTO destination_db.dbo.$destinationTableName (
        $columnList
        , warehouse_isDeleted
        , latest_updated
    )
    SELECT 
        $columnList
        , 0 AS warehouse_isDeleted
        , NULL AS latest_updated
    FROM tempdb..$tempTableName;
    ";

        $this->info("Executing query:\n$query");
        DB::connection('sqlsrv_destination')->unprepared($query);
        $this->info("Copied $tableName from tempdb to destination_db with additional columns.");
    }
    private function dropTempTable($tableName)
    {
        $tempTableName = "##" . str_replace('-', '_', $tableName);
        $query = "IF OBJECT_ID('tempdb..$tempTableName') IS NOT NULL DROP TABLE $tempTableName";
        DB::connection('sqlsrv_tempdb')->unprepared($query);
        $this->info("Dropped temporary table $tempTableName.");
    }
    private function logJobDetails($tableName, $jobId, $isSuccessful, $error, $executionTime)
    {
        DB::connection('sqlsrv')->table('job_tables')->insert([
            'job_id' => $jobId,
            'table_name' => $tableName,
            'message' => $error,
            'execution_time' => $executionTime,
            'status' => $isSuccessful,
            'created_at' => Carbon::now(),
            'updated_at' => Carbon::now(),
        ]);
    }
}
