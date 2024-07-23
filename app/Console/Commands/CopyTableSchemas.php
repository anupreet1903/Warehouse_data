<?php

namespace App\Console\Commands;

use App\Models\Jobs;
use Carbon\Carbon;
use Exception;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class CopyTableSchemas extends Command
{
    protected $signature = 'copy:table-schema';
    protected $description = 'Copy table schemas from source DB to destination DB via tempdb';

    protected $sourceConnection = 'sqlsrv_source';
    protected $destinationConnection = 'sqlsrv_destination';
    protected $tempdbConnection = 'sqlsrv_tempdb';

    public function __construct()
    {
        parent::__construct();
    }

    public function handle()
    {
        $destinationDbCredId = 2;
        $tables = ['BackendMasterAgent', 'BackendOrderStatus'];


        foreach ($tables as $table) {
            try {
                $this->copyTable($table, $destinationDbCredId);
            } catch (Exception $e) {
                $this->error("Error processing table $table: " . $e->getMessage());
                $this->logJobDetails($table, $destinationDbCredId, false, $e->getMessage());
            }
        }

        $this->info('Table copy process completed.');
    }

    private function copyTable($table, $destinationDbCredId)
    {
        $jobName = Jobs::find($signature);
        $startTime = microtime(true);

        // Step 1: Get column information
        $columns = $this->getColumns($table);

        // Step 2: Copy from source to tempdb
        $this->copyToTempDb($table, $columns);

        // $this->info("Temporary table created. Press Enter to continue...");
        // fgets(STDIN);

        // Step 3: Copy from tempdb to destination
        $this->copyFromTempDbToDestination($table, $columns);

        // Step 4: Drop temporary table
        $this->dropTempTable($table);

        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;

        $this->logJobDetails($table, $destinationDbCredId, true, null, $executionTime);
    }

    private function getColumns($table)
    {
        $columnsQuery = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
                          FROM INFORMATION_SCHEMA.COLUMNS
                          WHERE TABLE_NAME = '$table'
                          ORDER BY ORDINAL_POSITION";

        return DB::connection('sqlsrv_source')->select($columnsQuery);
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

    private function copyToTempDb($table, $columns)
    {
        $columnList = implode(', ', array_map(function ($col) {
            return "[{$col->COLUMN_NAME}]";
        }, $columns));

        $query = "SELECT $columnList INTO tempdb..##$table FROM source_db.dbo.$table";
        DB::connection('sqlsrv_source')->unprepared($query);
        $this->info("Copied $table to tempdb.");
    }

    private function copyFromTempDbToDestination($table, $columns)
    {
        $columnDefinitions = implode(', ', array_map([$this, 'formatColumnDefinition'], $columns));
        $columnList = implode(', ', array_map(function ($col) {
            return "[{$col->COLUMN_NAME}]";
        }, $columns));

        $query = "IF OBJECT_ID('destination_db.dbo.$table', 'U') IS NOT NULL DROP TABLE destination_db.dbo.$table;
                   CREATE TABLE destination_db.dbo.$table ($columnDefinitions);
                   INSERT INTO destination_db.dbo.$table ($columnList)
                   SELECT $columnList FROM tempdb..##$table;";

        $this->info("Executing query:\n$query");
        DB::connection('sqlsrv_destination')->unprepared($query);
        $this->info("Copied $table from tempdb to destination_db.");
    }

    private function dropTempTable($table)
    {
        $query = "IF OBJECT_ID('tempdb..##$table') IS NOT NULL DROP TABLE ##$table";
        DB::connection('sqlsrv_tempdb')->unprepared($query);
        $this->info("Dropped temporary table ##$table.");
    }

    private function logJobDetails($tableName, $destinationDbCredId, $isSuccessful, $errorMessage = null, $executionTime = null)
    {
        DB::connection('sqlsrv')->table('job_tables')->insert([
            'job_id' => 1,
            'table_name' => $tableName,
            'message' => $errorMessage,
            'status' => $isSuccessful ? true : false,
            'created_at' => Carbon::now(),
            'updated_at' => Carbon::now(),
        ]);
    }
}
