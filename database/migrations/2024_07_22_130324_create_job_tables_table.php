<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        Schema::connection('sqlsrv')->create('job_tables', function (Blueprint $table) {
            $table->id();
            $table->unsignedBigInteger('job_id'); // Foreign key column
            $table->foreign('job_id')->references('job_id')->on('jobs')->onDelete('cascade');
            $table->string('table_name')->nullable();
            $table->integer('status')->default(0);
            $table->string('message')->nullable();
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        Schema::dropIfExists('job_tables');
    }
};
