#ifndef __SQL_STATISTICS_H__
#define __SQL_STATISTICS_H__

#include "my_global.h"
#include "sql_profile.h"
#include "mysql/psi/mysql_thread.h"

#define MD5_SIZE 16
#define MAX_SQL_COUNT 5000
#define MAX_DBNAME_SIZE 32
#define MAX_SQL_TEXT_SIZE 1024
#define MD5_HASH_TO_STRING_LENGTH 32

#define MD5_HASH_TO_STRING(_hash, _str)                    \
  sprintf(_str, "%02x%02x%02x%02x%02x%02x%02x%02x"         \
                "%02x%02x%02x%02x%02x%02x%02x%02x",        \
          _hash[0], _hash[1], _hash[2], _hash[3],          \
          _hash[4], _hash[5], _hash[6], _hash[7],          \
          _hash[8], _hash[9], _hash[10], _hash[11],        \
          _hash[12], _hash[13], _hash[14], _hash[15])

extern ulong statistics_max_sql_size;
extern ulong statistics_max_sql_count;
extern ulong statistics_output_cycle;
extern ulong statistics_expire_duration;
extern my_bool output_thread_exit;
extern my_bool statistics_enabled;
extern my_bool statistics_output_now;
extern my_bool statistics_shutdown_fast;
extern char *statistics_exclude_db;
extern char *statistics_exclude_sql;
extern my_bool statistics_plugin_status;

/* queue for store index thread safety */
template<class T> class SQueue : public Queue<T>
{
public:
  SQueue():Queue<T>()
  {
    //mysql_mutex_init(0, &mlock, MY_MUTEX_INIT_FAST);
  }
  virtual ~SQueue()
  {
    mysql_mutex_destroy(&mlock);
  }

  void init()
  {
	mysql_mutex_init(0, &mlock, MY_MUTEX_INIT_FAST);
  }

  inline void push_back(T *a)
  {
    mysql_mutex_lock(&mlock);
    Queue<T>::push_back(a);
    mysql_mutex_unlock(&mlock);
  }
  inline T* pop()
  {
    T *ret = NULL;
    mysql_mutex_lock(&mlock);
    ret = (T*) Queue<T>::pop();
    mysql_mutex_unlock(&mlock);
    return ret;
  }
private:
  mysql_mutex_t mlock;
};

typedef struct index_info
{
  LEX_STRING index_name;    /* the index name witch the SQL accessed */
  uint       index_reads;   /* accessed times of the index name */
}INDEX_INFO;

/* class for statistics, every thd has one SQLInfo */
class SQLInfo
{
public:
  SQLInfo():sql_format(NULL), logical_reads(0), is_full(false),exclude(false),is_stopped(false),
            sql_byte_count(0), last_id_index(0), or_index(0), left_brackets_after_or(0),
            or_end_index(0), memory_temp_table_created(0), disk_temp_table_created(0),row_reads(0),
	  byte_reads(0), charset_number(0) {
	  index_queue.init();
  }
  unsigned char *sql_format;    /* store the format of the SQL */
  ulonglong     logical_reads;  /* logical reads by the SQL */
  bool          is_full;        /* match the max size of the SQL*/
  bool          exclude;        /* exclude the SQL, do not store the SQL witch this flag set true */
  bool          is_stopped;     /* if this flag is true, topSQL is off */
  uint          sql_byte_count; /* scan bytes by this SQL */
  uint          last_id_index;  /* the index of the last id token in sql_format */
  uint          or_index;       /* the index of keyword 'or' */
  uint          left_brackets_after_or; /* the '(' after keyword 'or', maybe the condition is like or (...) */
  uint          or_end_index;   /* the end of the or condition */
  uint          memory_temp_table_created; /* memory temp tables created by this SQL */
  uint          disk_temp_table_created; /* disk temp tables created by this SQL */
  ulonglong     start_time;              /* start time for current statistics */
  ulong         row_reads;               /* rows read by this SQL */
  ulong         byte_reads;              /* bytes read by this SQL */
  uint          charset_number;
  SQueue<INDEX_INFO>  index_queue;      /* list of indexes used by this SQL */
};

/* class used for hash table*/
class Statistics
{
public:
  Statistics(SQLInfo *info);
  virtual ~Statistics();
  Statistics &operator +=(Statistics &s);
  void reset();
  unsigned char *m_sql_format;
  size_t        length;         /* used by hash table */
  size_t		sql_text_length;
  ulonglong     m_logical_reads;
  ulong         m_memory_temp_table_created;
  ulong         m_disk_temp_table_created;
  ulong         m_row_reads;
  ulong         m_byte_reads;
  ulong         m_exec_times; /* all the SQL witch has the same format execute times */
  ulong         m_max_exec_times; /* the max execute time of the SQL with same format */
  ulong         m_min_exec_times; /* the min execute time of the SQL with same format */
  ulong         m_exec_count;     /* the count of the SQL exexutes with same format */
  uint          m_charset_number;
  SQueue<INDEX_INFO> m_index_queue;
};

typedef struct Table_Info
{
  char table_name[NAME_CHAR_LEN * 2 + 1];
  enum_sql_command command;
}TableInfo;

class TableStats
{
public:
  TableStats(const TableInfo &info);
  TableStats& operator +=(const TableInfo &info)
  {
    switch(info.command)
    {
      case SQLCOM_SELECT:
        select_count++;
        break;
      case SQLCOM_INSERT:
        insert_count++;
        break;
      case SQLCOM_UPDATE:
        update_count++;
        break;
      case SQLCOM_DELETE:
        delete_count++;
        break;
      default:break;
    }
    return *this;
  }
public:
  char table_name[NAME_CHAR_LEN * 2 + 1];
  uint  length;
  ulong select_count;
  ulong insert_count;
  ulong update_count;
  ulong delete_count;
};

#define INCREASE_MEM_TEMP_TABLE_CREATED(thd) \
  if (thd->m_sql_info != NULL) \
  thd->m_sql_info->memory_temp_table_created++

#define INCREASE_DISK_TEMP_TABLE_CREATED(thd) \
  if (thd->m_sql_info != NULL) \
  thd->m_sql_info->disk_temp_table_created++

#define INCREASE_LOGICAL_READS(thd, reads)	\
  if (thd->m_sql_info != NULL)              \
  thd->m_sql_info->logical_reads += reads

#define INCREASE_ROW_BYTE_READS(thd, bytes)	\
  if (thd->m_sql_info != NULL)  \
{                             \
  thd->m_sql_info->byte_reads += bytes; \
  thd->m_sql_info->row_reads ++;\
}

#define INIT_START_TIME(thd)	\
  if (thd->m_sql_info != NULL) \
  thd->m_sql_info->start_time = my_micro_time()

#define GET_QUERY_EXEC_TIME(info)	\
  (info != NULL ? (my_micro_time() - info->start_time) : 0);

#define EXCLUDE_CURRENT_SQL(thd)  \
  if (thd->m_sql_info != NULL)  \
  thd->m_sql_info->exclude = TRUE

class Modification_plan;
void output_now(bool output);
bool update_exclude_db_list();
bool update_exclude_sql_list();
void statistics_add_token(SQLInfo* info, uint token, void *yylval);
void statistics_exclude_current_sql(THD *thd);
void statistics_destory_sql_info(SQLInfo *info);
void statistics_reset_sql_info(SQLInfo *info, uint charset);
bool statistics_show_table_stats(THD *thd);
bool statistics_show_sql_stats(uint counts, THD *thd);
bool statistics_show_status(THD *thd);
int  statistics_init();
int  statistics_deinit();
SQLInfo *statistics_create_sql_info(uint charset);
void statistics_end_sql_statement(THD *thd);
void statistics_start_sql_statement(THD *thd);
void statistics_save_index(JOIN *join);
void statistics_save_index(THD *thd,  TABLE *table ,Modification_plan *plan);

#endif // __SQL_STATISTICS_H__
