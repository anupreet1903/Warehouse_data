<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class Jobs extends Model
{
    use HasFactory;
    protected $connection = 'sqlsrv';
    protected $table = 'jobs';
    
    protected $fillable = [
        'job_name',
        'isEnabled',
        'created_at',
        'updated_at',
    ];
}
