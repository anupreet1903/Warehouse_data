<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class JobTables extends Model
{
    use HasFactory;
    protected $connection = 'sqlsrv';
    protected $table = 'job_tables';
    
    protected $fillable = [
        'job_id',
        'table_name',
        'status',
        'message',
        'created_at',
        'updated_at',
    ];
}
