<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class LogDetails extends Model
{
    use HasFactory;
    protected $connection = 'sqlsrv';
    protected $table = 'log_details';
    
    protected $fillable = [
        'info_type',
        'table_name',
        'message',
        'created_at',
        'updated_at',
    ];
}
