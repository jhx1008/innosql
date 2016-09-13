#define MYSQL_LEX 1
#include "sql_class.h"
#include "stat_lex_token.h"
#include "records.h"
#include "sql_lex.h"
#include "my_md5.h"
#include "sql_base.h"
#include "../sql/sql_yacc.h"
#include "sql_statistics.h"
#include "sql_select.h"
#include "sql_optimizer.h"
#include <ctype.h>
#include "opt_explain.h"
#include "mysql/thread_pool_priv.h"
#include "mysqld_thd_manager.h"

#ifdef LEX_YYSTYPE
#undef LEX_YYSTYPE
#endif

#define LEX_YYSTYPE YYSTYPE
#define SECONDS_PER_HOUR (60*60)
#define SECONDS_PER_DAY  (60*60*24)


#define SIZE_OF_A_TOKEN 2

ulong statistics_max_sql_size = 1024;
ulong statistics_max_sql_count = 100;
ulong statistics_output_cycle  = 1;
ulong statistics_expire_duration = 3;
char* statistics_exclude_db = NULL;
char* statistics_exclude_sql = NULL;
my_bool output_thread_exit       = FALSE;
my_bool statistics_output_now    = FALSE;
my_bool statistics_shutdown_fast = FALSE;
my_bool statistics_plugin_status = FALSE;

static my_bool output_thread_running = FALSE;
static ulong start_output_time = 0;
static ulong end_output_time = 0;

static mysql_rwlock_t  exclude_queue_lock;
static SQueue<char> statistics_exclude_db_queue;

static mysql_rwlock_t  exclude_sql_queue_lock;
static SQueue<char> statistics_exclude_sql_queue;

static HASH           stat_sql_hash;
static mysql_mutex_t  hash_sql_lock;

static HASH           stat_table_hash;
static mysql_mutex_t  hash_table_lock;

static mysql_mutex_t  stat_output_lock;
static mysql_cond_t   stat_output_cond;

static const char *sql_command[] = {
	"SQLCOM_SELECT","SQLCOM_CREATE_TABLE","SQLCOM_CREATE_INDEX","SQLCOM_ALTER_TABLE","SQLCOM_UPDATE","SQLCOM_INSERT","SQLCOM_INSERT_SELECT","SQLCOM_DELETE","SQLCOM_TRUNCATE",
	"SQLCOM_DROP_TABLE","SQLCOM_DROP_INDEX","SQLCOM_SHOW_DATABASES","SQLCOM_SHOW_TABLES","SQLCOM_SHOW_FIELDS","SQLCOM_SHOW_KEYS","SQLCOM_SHOW_VARIABLES","SQLCOM_SHOW_STATUS",
	"SQLCOM_SHOW_ENGINE_LOGS","SQLCOM_SHOW_ENGINE_STATUS","SQLCOM_SHOW_ENGINE_MUTEX","SQLCOM_SHOW_PROCESSLIST","SQLCOM_SHOW_MASTER_STAT","SQLCOM_SHOW_SLAVE_STAT","SQLCOM_SHOW_GRANTS",
	"SQLCOM_SHOW_CREATE","SQLCOM_SHOW_CHARSETS","SQLCOM_SHOW_COLLATIONS","SQLCOM_SHOW_CREATE_DB","SQLCOM_SHOW_TABLE_STATUS","SQLCOM_SHOW_TRIGGERS","SQLCOM_LOAD","SQLCOM_SET_OPTION",
	"SQLCOM_LOCK_TABLES","SQLCOM_UNLOCK_TABLES","SQLCOM_GRANT","SQLCOM_CHANGE_DB","SQLCOM_CREATE_DB","SQLCOM_DROP_DB","SQLCOM_ALTER_DB","SQLCOM_REPAIR","SQLCOM_REPLACE","SQLCOM_REPLACE_SELECT",
	"SQLCOM_CREATE_FUNCTION","SQLCOM_DROP_FUNCTION","SQLCOM_REVOKE","SQLCOM_OPTIMIZE","SQLCOM_CHECK","SQLCOM_ASSIGN_TO_KEYCACHE","SQLCOM_PRELOAD_KEYS","SQLCOM_FLUSH","SQLCOM_KILL",
	"SQLCOM_ANALYZE""SQLCOM_ROLLBACK","SQLCOM_ROLLBACK_TO_SAVEPOINT","SQLCOM_COMMIT","SQLCOM_SAVEPOINT","SQLCOM_RELEASE_SAVEPOINT","SQLCOM_SLAVE_START","SQLCOM_SLAVE_STOP",
	"SQLCOM_START_GROUP_REPLICATION","SQLCOM_STOP_GROUP_REPLICATION","SQLCOM_BEGIN","SQLCOM_CHANGE_MASTER","SQLCOM_CHANGE_REPLICATION_FILTER","SQLCOM_RENAME_TABLE","SQLCOM_RESET",
	"SQLCOM_PURGE","SQLCOM_PURGE_BEFORE","SQLCOM_SHOW_BINLOGS","SQLCOM_SHOW_OPEN_TABLES","SQLCOM_HA_OPEN","SQLCOM_HA_CLOSE","SQLCOM_HA_READ","SQLCOM_SHOW_SLAVE_HOSTS",
	"SQLCOM_DELETE_MULTI","SQLCOM_UPDATE_MULTI","SQLCOM_SHOW_BINLOG_EVENTS","SQLCOM_DO","SQLCOM_SHOW_WARNS","SQLCOM_EMPTY_QUERY","SQLCOM_SHOW_ERRORS","SQLCOM_SHOW_STORAGE_ENGINES",
	"SQLCOM_SHOW_PRIVILEGES","SQLCOM_HELP","SQLCOM_CREATE_USER","SQLCOM_DROP_USER","SQLCOM_RENAME_USER","SQLCOM_REVOKE_ALL","SQLCOM_CHECKSUM","SQLCOM_CREATE_PROCEDURE","SQLCOM_CREATE_SPFUNCTION",
	"SQLCOM_CALL","SQLCOM_DROP_PROCEDURE","SQLCOM_ALTER_PROCEDURE","SQLCOM_ALTER_FUNCTION","SQLCOM_SHOW_CREATE_PROC","SQLCOM_SHOW_CREATE_FUNC","SQLCOM_SHOW_STATUS_PROC","SQLCOM_SHOW_STATUS_FUNC",
	"SQLCOM_PREPARE","SQLCOM_EXECUTE","SQLCOM_DEALLOCATE_PREPARE","SQLCOM_CREATE_VIEW","SQLCOM_DROP_VIEW","SQLCOM_CREATE_TRIGGER","SQLCOM_DROP_TRIGGER","SQLCOM_XA_START",
	"SQLCOM_XA_END","SQLCOM_XA_PREPARE","SQLCOM_XA_COMMIT","SQLCOM_XA_ROLLBACK","SQLCOM_XA_RECOVER","SQLCOM_SHOW_PROC_CODE","SQLCOM_SHOW_FUNC_CODE","SQLCOM_ALTER_TABLESPACE",
	"SQLCOM_INSTALL_PLUGIN","SQLCOM_UNINSTALL_PLUGIN","SQLCOM_BINLOG_BASE64_EVENT","SQLCOM_SHOW_PLUGINS","SQLCOM_CREATE_SERVER","SQLCOM_DROP_SERVER","SQLCOM_ALTER_SERVER",
	"SQLCOM_CREATE_EVENT","SQLCOM_ALTER_EVENT","SQLCOM_DROP_EVENT","SQLCOM_SHOW_CREATE_EVENT","SQLCOM_SHOW_EVENTS","SQLCOM_SHOW_CREATE_TRIGGER","SQLCOM_ALTER_DB_UPGRADE",
	"SQLCOM_SHOW_PROFILE","SQLCOM_SHOW_PROFILES","SQLCOM_SIGNAL","SQLCOM_RESIGNAL","SQLCOM_SHOW_RELAYLOG_EVENTS","SQLCOM_GET_DIAGNOSTICS","SQLCOM_SHOW_SQL_STATS",
	"SQLCOM_SHOW_TABLE_STATS","SQLCOM_SHOW_STATISTICS_STATUS","SQLCOM_ALTER_USER","SQLCOM_EXPLAIN_OTHER","SQLCOM_SHOW_CREATE_USER","SQLCOM_SHUTDOWN","SQLCOM_ALTER_INSTANCE"
};

/* use for sort */
struct STATISTICS_ARRAY{
  uint        length;
  Statistics  *array[MAX_SQL_COUNT];
}statistics_array;

struct STATUS_VARIABLES{
  int64         discard_because_exclude;
  int64         discard_because_too_long;
  ulong         output_counts;
  ulong         clean_counts;
  int64         oos_sql_counts; /* out of size sql counts*/
  int64         sql_enter_counts;
  int64         table_enter_counts;
  ulonglong     discard_because_hashtable_full;
  mysql_mutex_t status_lock;
}status_variables;

Statistics::Statistics(SQLInfo *info):m_logical_reads(info->logical_reads),
                                      m_memory_temp_table_created(info->memory_temp_table_created),
                                      m_disk_temp_table_created(info->disk_temp_table_created),
                                      m_row_reads(info->row_reads),m_byte_reads(info->byte_reads),
                                      m_exec_times(0),m_max_exec_times(0),m_min_exec_times(0),
                                      m_charset_number(info->charset_number),length(info->sql_byte_count),
                                      m_exec_count(1),sql_text_length(info->sql_byte_count)
{
  m_sql_format = new unsigned char[MAX_SQL_TEXT_SIZE + MAX_DBNAME_SIZE];
  memcpy(m_sql_format, info->sql_format, MAX_SQL_TEXT_SIZE);
  INDEX_INFO *index_info = NULL;
  m_index_queue.init();
  while((index_info = info->index_queue.pop()) != NULL)
  {
    m_index_queue.push_back(index_info);
  }
}

Statistics::~Statistics()
{
  if (m_sql_format != NULL)
  {
    INDEX_INFO *index_info = NULL;
    while((index_info = m_index_queue.pop()) != NULL)
    {
      my_free(index_info->index_name.str);
      my_free(index_info);
    }
    delete [] m_sql_format;
    m_sql_format = NULL;
  }
}

void Statistics::reset()
{
  m_logical_reads = 0;
  m_memory_temp_table_created = 0;
  m_disk_temp_table_created = 0;
  m_row_reads = 0;
  m_byte_reads = 0;
  m_exec_times = 0;
  m_max_exec_times = 0;
  m_min_exec_times = 0;
  m_exec_count = 0;
  INDEX_INFO *index_info = NULL;
  while((index_info = m_index_queue.pop()) != NULL)
  {
    my_free(index_info->index_name.str);
    my_free(index_info);
  }
}

Statistics& Statistics::operator+=(Statistics &s)
{
  m_logical_reads += s.m_logical_reads;
  m_memory_temp_table_created += s.m_memory_temp_table_created;
  m_disk_temp_table_created += s.m_disk_temp_table_created;
  m_row_reads += s.m_row_reads;
  m_byte_reads += s.m_byte_reads;
  m_exec_times += s.m_exec_times;
  /*max and min exec time not do the +*/
  m_max_exec_times = m_max_exec_times > s.m_max_exec_times ? m_max_exec_times : s.m_max_exec_times;
  m_min_exec_times = m_min_exec_times < s.m_min_exec_times ? m_min_exec_times : s.m_min_exec_times;
  m_exec_count += s.m_exec_count;
  INDEX_INFO *index_info = NULL;
  while ((index_info = s.m_index_queue.pop()) != NULL)
  {
    void *dst_queue_node = m_index_queue.new_iterator();
    while(dst_queue_node != NULL)
    {
      INDEX_INFO *dst_index_info = m_index_queue.iterator_value(dst_queue_node);
      /* if the has the same index, do the operator +, else add the index into queue */
      if ((index_info->index_name.length == dst_index_info->index_name.length) &&
        strcmp(index_info->index_name.str,dst_index_info->index_name.str) == 0)
      {
        dst_index_info->index_reads += index_info->index_reads;
        my_free(index_info->index_name.str);
        my_free(index_info);
        break;
      }
      dst_queue_node = m_index_queue.iterator_next(dst_queue_node);
    }
    if (dst_queue_node == NULL)
    {
      m_index_queue.push_back(index_info);
    }
  }
  return *this;
}

TableStats::TableStats(const TableInfo &info)
{
  memset(table_name, 0, sizeof(table_name));
  length = strlen(info.table_name);
  memcpy(table_name, info.table_name, length);
  select_count = update_count = insert_count = delete_count = 0;
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
}

void output_now(bool output)
{
  if (output)
  {
    mysql_cond_signal(&stat_output_cond);
  }
}

bool update_exclude_db_list()
{
  mysql_rwlock_wrlock(&exclude_queue_lock);
  char *db_name = NULL;
  while((db_name = statistics_exclude_db_queue.pop()) != NULL)
  {
    my_free(db_name);
  }
  if (statistics_exclude_db != NULL)
  {
    char *temp = my_strdup(key_memory_top_sql, statistics_exclude_db, MYF(MY_WME));
    char *token = NULL, *saveptr = NULL;
    for (token = my_strtok_r(temp, ";", &saveptr); token; token= my_strtok_r(NULL, ";", &saveptr))
    {
      char *name = my_strdup(key_memory_top_sql, token, MYF(MY_WME));
      statistics_exclude_db_queue.push_back(name);
    }
    my_free(temp);
  }
  mysql_rwlock_unlock(&exclude_queue_lock);
  return false;
}

bool update_exclude_sql_list()
{
  mysql_rwlock_wrlock(&exclude_sql_queue_lock);
  char *sql = NULL;
  while((sql = statistics_exclude_sql_queue.pop()) != NULL)
  {
    my_free(sql);
  }
  if (statistics_exclude_sql != NULL)
  {
    char *temp = my_strdup(key_memory_top_sql, statistics_exclude_sql, MYF(MY_WME));
    char *token = NULL, *saveptr = NULL;
    for (token = my_strtok_r(temp, ";", &saveptr); token; token= my_strtok_r(NULL, ";", &saveptr))
    {
      sql = my_strdup(key_memory_top_sql, token, MYF(MY_WME));
      for(uint i = 0; i < strlen(sql); i++)
      {
        sql[i] = toupper(sql[i]);
      }
      statistics_exclude_sql_queue.push_back(sql);
    }
    my_free(temp);
  }
  mysql_rwlock_unlock(&exclude_sql_queue_lock);
  return false;
}

extern "C" 
uchar *get_key_tablestats(TableStats *t, size_t *length,
                         my_bool not_used __attribute__((unused)))
{
  *length = t->length;
  return (uchar*)t->table_name;
}

extern "C"
void free_tablestats(TableStats *t)
{
  delete t; t = NULL;
}

extern "C" 
uchar *get_key_statistics(Statistics *s, size_t *length,
                           my_bool not_used __attribute__((unused)))
{
  *length = s->length;
  return (uchar*)s->m_sql_format;
}

extern "C"
void free_statistics(Statistics *s)
{
  delete s; s = NULL;
}

static void init_statistics_hash(void)
{
  mysql_rwlock_init(0, &exclude_queue_lock);
  mysql_rwlock_init(0, &exclude_sql_queue_lock);
  mysql_mutex_init(0, &hash_sql_lock, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(0, &hash_table_lock, MY_MUTEX_INIT_FAST);
#ifndef NO_EMBEDDED_ACCESS_CHECKS
  (void)my_hash_init(&stat_sql_hash, system_charset_info, statistics_max_sql_count,
    0, 0,(my_hash_get_key)get_key_statistics, (my_hash_free_key)free_statistics, 0, key_memory_top_sql);
  (void)my_hash_init(&stat_table_hash, system_charset_info, 500,
    0, 0,(my_hash_get_key)get_key_tablestats, (my_hash_free_key)free_tablestats, 0, key_memory_top_sql);
#endif
}

static void
destory_statistics_hash(void)
{
  mysql_rwlock_destroy(&exclude_queue_lock);
  mysql_rwlock_destroy(&exclude_sql_queue_lock);
  mysql_mutex_destroy(&hash_sql_lock);
  mysql_mutex_destroy(&hash_table_lock);
  my_hash_free(&stat_sql_hash);
  my_hash_free(&stat_table_hash);
}

static bool statistics_cmp(const Statistics *s1, const Statistics *s2)
{
  return s1->m_exec_times > s2->m_exec_times;
}

/* sort the statistics array for the 'show top sql' command, order by the execute time desc */
static void quick_sort(Statistics **elem, const int &start, const int &end, bool(*cmp)(const Statistics *, const Statistics*))
{
  int i = start, j = end;
  const Statistics *m = *(elem + ((start + end) >> 1));
  Statistics *temp;
  do
  {
    while (cmp(elem[i], m)) i++;
    while (cmp(m, elem[j])) j--;
    if (i <= j)
    {
      temp = elem[i]; elem[i] = elem[j]; elem[j] = temp;
      i++; j--;
    }
  }while (i < j);
  if (i < end) quick_sort(elem, i, end, cmp);
  if (start < j) quick_sort(elem, start, j, cmp);
}

static bool contain_exclude_database(TABLE_LIST *tables)
{
  if (tables == NULL) return FALSE;
  TABLE_LIST *table = NULL;
  mysql_rwlock_rdlock(&exclude_queue_lock);
  for(table = tables; table; table = table->next_global)
  {
    void *dst_queue_node = statistics_exclude_db_queue.new_iterator();
    while(dst_queue_node != NULL)
    {
      char *db_name = statistics_exclude_db_queue.iterator_value(dst_queue_node);
      if (strcmp(db_name, table->db) == 0)
      {
        mysql_rwlock_unlock(&exclude_queue_lock);
        return TRUE;
      }
      dst_queue_node = statistics_exclude_db_queue.iterator_next(dst_queue_node);
    }
  }
  mysql_rwlock_unlock(&exclude_queue_lock);
  return FALSE;
}

static bool is_exclude_database(const char *dbName)
{
  if (dbName == NULL) return FALSE;
  mysql_rwlock_rdlock(&exclude_queue_lock);
  void *dst_queue_node = statistics_exclude_db_queue.new_iterator();
  while(dst_queue_node != NULL)
  {
    char *db_name = statistics_exclude_db_queue.iterator_value(dst_queue_node);
    if (strcmp(db_name, dbName) == 0)
    {
      mysql_rwlock_unlock(&exclude_queue_lock);
      return TRUE;
    }
    dst_queue_node = statistics_exclude_db_queue.iterator_next(dst_queue_node);
  }
  mysql_rwlock_unlock(&exclude_queue_lock);
  return FALSE;
}

static bool is_exclude_sql(const char *sqlType)
{
  if (sqlType == NULL) return FALSE;
  mysql_rwlock_rdlock(&exclude_sql_queue_lock);
  void *dst_queue_node = statistics_exclude_sql_queue.new_iterator();
  while (dst_queue_node != NULL)
  {
    char *type = statistics_exclude_sql_queue.iterator_value(dst_queue_node);
    char buffer[32] = {0};
    sprintf(buffer, "SQLCOM_%s", type);
    if (strncmp(sqlType, buffer, strlen(type) + 7) == 0)
    {
      mysql_rwlock_unlock(&exclude_sql_queue_lock);
      return TRUE;
    }
    dst_queue_node = statistics_exclude_sql_queue.iterator_next(dst_queue_node);
  }
  mysql_rwlock_unlock(&exclude_sql_queue_lock);
  return FALSE;
}

static void statistics_sort_by_exectime(Statistics **s, uint len)
{
  if (len < 2) return;
  quick_sort(s, 0, len - 1, statistics_cmp);
}

static int read_token(const SQLInfo *info, uint index, uint *token)
{
  DBUG_ENTER("read token sql info");
  DBUG_ASSERT(index <= info->sql_byte_count);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);
  if (index + SIZE_OF_A_TOKEN <= info->sql_byte_count)
  {
    const unsigned char *src = &info->sql_format[index];
    *token = src[0] | (src[1] << 8);
    DBUG_RETURN(index + SIZE_OF_A_TOKEN);
  }
  *token = 0;
  DBUG_RETURN(MAX_SQL_TEXT_SIZE + 1);
}

static int read_token(const Statistics *s, uint index, uint *token)
{
  DBUG_ENTER("read token statistics");
  DBUG_ASSERT(index <= s->length);
  DBUG_ASSERT(s->length <= MAX_SQL_TEXT_SIZE);

  if (index + SIZE_OF_A_TOKEN <= s->length)
  {
    const unsigned char *src = &s->m_sql_format[index];
    *token = src[0] | (src[1] << 8);
    DBUG_RETURN(index + SIZE_OF_A_TOKEN);
  }
  *token = 0;
  DBUG_RETURN(MAX_SQL_TEXT_SIZE + 1);
}

static void store_token(SQLInfo *info, uint token)
{
  DBUG_ENTER("store token");
  DBUG_ASSERT(info->sql_byte_count >= 0);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);
  if (info->sql_byte_count + SIZE_OF_A_TOKEN <= MAX_SQL_TEXT_SIZE)
  {
    unsigned char *dest = info->sql_format + info->sql_byte_count;
    dest[0] = token & 0xff;
    dest[1] = (token >> 8) & 0xff;
    info->sql_byte_count += SIZE_OF_A_TOKEN;
  }
  else
  {
    info->is_full = true;
    my_atomic_add64(&status_variables.oos_sql_counts, 1);
    //thread_safe_increment(status_variables.oos_sql_counts, &status_variables.status_lock);
  }
  DBUG_VOID_RETURN;
}

static uint peek_token(const SQLInfo *info, uint index)
{
  uint token;
  DBUG_ENTER("peek token");
  DBUG_ASSERT(index >= 0);
  DBUG_ASSERT(index + SIZE_OF_A_TOKEN <= info->sql_byte_count);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);
  token = ((info->sql_format[index + 1]) << 8) | info->sql_format[index];
  DBUG_RETURN(token);
}

static void peek_last_two_tokens(const SQLInfo *info, uint *t1, uint *t2)
{
  DBUG_ENTER("peek last two tokens");
  int last_id_index = info->last_id_index;
  int byte_count = info->sql_byte_count;
  if (last_id_index <= byte_count - SIZE_OF_A_TOKEN)
  {
    *t1 = peek_token(info, byte_count - SIZE_OF_A_TOKEN);
  }
  else
  {
    *t1 = TOK_STAT_UNUSED;
  }
  if (last_id_index <= byte_count - 2 * SIZE_OF_A_TOKEN)
  {
    *t2 = peek_token(info, byte_count - 2 * SIZE_OF_A_TOKEN);
  }
  else
  {
    *t1 = TOK_STAT_UNUSED;
  }
  DBUG_VOID_RETURN;
}

static int read_identifier(SQLInfo *info, uint index, char **id_string, int *id_length)
{
  uint new_index;
  DBUG_ENTER("read indextifier sql info");
  DBUG_ASSERT(index <= info->sql_byte_count);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);

  unsigned char *src = &info->sql_format[index];
  uint length = src[0] | (src[1] << 8);
  *id_string = (char *)(src + 2);
  *id_length = length;

  new_index = index + SIZE_OF_A_TOKEN + length;
  DBUG_ASSERT(new_index <= info->sql_byte_count);
  DBUG_RETURN(new_index);
}

static int read_identifier(Statistics *s, uint index, char **id_string, int *id_length)
{
  uint new_index;
  DBUG_ENTER("read identifier statistics");
  DBUG_ASSERT(index <= s->length);
  DBUG_ASSERT(s->length <= MAX_SQL_TEXT_SIZE);

  unsigned char *src = &s->m_sql_format[index];
  uint length = src[0] | (src[1] << 8);
  *id_string = (char *)(src + 2);
  *id_length = length;

  new_index = index + SIZE_OF_A_TOKEN + length;
  DBUG_ASSERT(new_index <= s->length);
  DBUG_RETURN(new_index);
}

static void store_token_identifier(SQLInfo *info,uint token, 
                                      uint id_length, const char *id_name)
{
  DBUG_ENTER("store token identifier");
  DBUG_ASSERT(info->sql_byte_count >= 0);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);
  uint bytes_needed = 2 * SIZE_OF_A_TOKEN + id_length;
  if (info->sql_byte_count + bytes_needed <= MAX_SQL_TEXT_SIZE)
  {
    unsigned char *dest = &info->sql_format[info->sql_byte_count];
    dest[0] = token & 0xff;
    dest[1] = (token >> 8) & 0xff;
    dest[2] = id_length & 0xff;
    dest[3] = (id_length >> 8) & 0xff;

    if (id_length > 0)
      memcpy((char*)(dest + 4), id_name, id_length);
    info->sql_byte_count += bytes_needed;
  }
  else
  {
    info->is_full = true;
  }
  DBUG_VOID_RETURN;
}


static void get_sql_text(char *text, char *dbname, Statistics *s)
{
  DBUG_ENTER("get sql text");
  DBUG_ASSERT(s != NULL);
  bool truncated = false;
  uint byte_count = s->sql_text_length;
  uint byte_needed = 0;
  uint tok = 0;
  uint current_byte = 0;
  lex_token_string *token_data;
  uint bytes_available = MAX_SQL_TEXT_SIZE - 4;
  if (s->sql_text_length == s->length){
	memcpy(dbname, "NULL", 4);
  }
  else {
	memcpy(dbname, s->m_sql_format + s->sql_text_length, s->length - s->sql_text_length);
  }
  
  CHARSET_INFO *from_cs = get_charset(s->m_charset_number, MYF(0));
  CHARSET_INFO *to_cs = &my_charset_utf8_bin;
  if (from_cs == NULL)
  {
    *text = '\0';
    DBUG_VOID_RETURN;
  }
  const uint max_converted_size = MAX_SQL_TEXT_SIZE * 4;
  char *id_buffer = (char*)my_malloc(key_memory_top_sql,max_converted_size,MYF(MY_WME));
  char *id_string;
  int id_length;
  bool convert_text = !my_charset_same(from_cs, to_cs);
  DBUG_ASSERT(byte_count <= MAX_SQL_TEXT_SIZE);

  while((current_byte < byte_count) &&
         (bytes_available > 0) &&
         !truncated)
  {
    current_byte = read_token(s, current_byte, &tok);
    token_data = &stat_lex_token_array[tok];
    switch(tok)
    {
      case IDENT:
      case IDENT_QUOTED:
      {
        char *id_ptr;
        int id_len;
        uint err_cs = 0;
        current_byte = read_identifier(s, current_byte, &id_ptr, &id_len);
        if (convert_text)
        {
          if (to_cs->mbmaxlen * id_len > max_converted_size)
          {
            truncated = true;
            break;
          }
          id_length = my_convert(id_buffer, max_converted_size, to_cs, 
                                  id_ptr, id_len, from_cs, &err_cs);
          id_string = id_buffer;
        }
        else
        {
          id_string = id_ptr;
          id_length = id_len;
        }

        if (id_length == 0 || err_cs != 0)
        {
          truncated = true;
          break;
        }

        byte_needed = id_length + (tok == IDENT ? 1 : 3);
        if (byte_needed <= bytes_available)
        {
          if (tok == IDENT_QUOTED)
            *text++='`';
          if (id_length > 0)
          {
            memcpy(text, id_string, id_length);
            text += id_length;
          }
          if (tok == IDENT_QUOTED)
            *text++='`';
          *text++=' ';
          bytes_available -= byte_needed;
        }
        else
        {
          truncated = true;
        }
        break;
      }
      default:
      {
        int tok_length = token_data->m_token_length;
        byte_needed = tok_length + 1;
        if (byte_needed <= bytes_available)
        {
          strncpy(text, token_data->m_token_string, tok_length);
          text += tok_length;
          *text++ = ' ';
          bytes_available -= byte_needed;
        }
        else
        {
          truncated = true;
        }
        break;
      }
    }
  }
  my_free(id_buffer);
  text = '\0';
  DBUG_VOID_RETURN;
}

/* if we find the or condition, we should check the values on the 
   left and right, if the values on left and right has the similar
   format like a=1 or a=2, we must convert it like this: a=?+, there
   is a example:
   select * from t where id=1 or id=2 or id=3, it will be converted
   like this:
   select * from t where id=?+
   this function will do the convert
 */
static void
stat_or_token_cmp(SQLInfo *info)
{
  DBUG_ENTER("stat or token cmp");
  uint or_right_start = info->or_index + SIZE_OF_A_TOKEN;
  uint or_left_start = info->or_index - (info->or_end_index - or_right_start);
  uint token_before_or;
  read_token(info, info->or_index - SIZE_OF_A_TOKEN,&token_before_or);
  if (token_before_or == '+')
  {
    or_left_start -= SIZE_OF_A_TOKEN;
  }
  if (or_left_start > info->or_index)
  {
    info->or_index = 0;
    DBUG_VOID_RETURN;
  }
  if (memcmp(&info->sql_format[or_left_start], &info->sql_format[or_right_start], info->or_end_index - or_right_start) == 0)
  {
    info->sql_byte_count = info->or_index;
    if (token_before_or != '+')
      store_token(info,'+');
  }
  info->or_index = 0;
  DBUG_VOID_RETURN;
}

/* this function maybe called when parse the sql, add the token into the token array */
void statistics_add_token(SQLInfo* info, uint token, void *yylval)
{
  DBUG_ENTER("statistics add token");
  if (info == NULL)
  {
    DBUG_VOID_RETURN;
  }
  info->is_stopped = !statistics_plugin_status;
  if (info->is_stopped || info->is_full || token == END_OF_INPUT)
    DBUG_VOID_RETURN;
  uint last_token;
  uint last_token2;

  peek_last_two_tokens(info, &last_token, &last_token2);

  switch(token)
  {
    case BIN_NUM:
    case DECIMAL_NUM:
    case FLOAT_NUM:
    case HEX_NUM:
    case LEX_HOSTNAME:
    case LONG_NUM:
    case NUM:
    case TEXT_STRING:
    case NCHAR_STRING:
    case ULONGLONG_NUM:
    {
      token = TOK_STAT_GENERIC_VALUE;
    }
    case NULL_SYM:
    {
      if ((last_token2 == TOK_STAT_GENERIC_VALUE ||
           last_token2 == TOK_STAT_GENERIC_VALUE_LIST ||
           last_token2 == NULL_SYM) &&
          (last_token == ','))
      {
        info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
        token = TOK_STAT_GENERIC_VALUE_LIST;
      }
      store_token(info, token);
      if (info->or_index != 0 && info->left_brackets_after_or == 0)
      {
        info->or_end_index = info->sql_byte_count;
        stat_or_token_cmp(info);
      }
      break;
    }
    case ')':
    {
      if (last_token == TOK_STAT_GENERIC_VALUE &&
           last_token2 == '(')
      {
        info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
        token = TOK_STAT_ROW_SINGLE_VALUE;

        peek_last_two_tokens(info, &last_token, &last_token2);

        if ((last_token2 == TOK_STAT_ROW_SINGLE_VALUE ||
             last_token2 == TOK_STAT_ROW_SINGLE_VALUE_LIST) &&
             (last_token == ','))
        {
          info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
          token = TOK_STAT_ROW_SINGLE_VALUE_LIST;
        }
      }
      else if (last_token == TOK_STAT_GENERIC_VALUE_LIST &&
               last_token2 == '(')
      {
        info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
        token = TOK_STAT_ROW_MULTIPLE_VALUE;

        peek_last_two_tokens(info, &last_token, &last_token2);

        if ((last_token2 == TOK_STAT_ROW_MULTIPLE_VALUE ||
             last_token2 == TOK_STAT_ROW_MULTIPLE_VALUE_LIST) &&
             (last_token == ','))
        {
          info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
          token = TOK_STAT_ROW_MULTIPLE_VALUE_LIST;
        }
      }
      store_token(info, token);
      if (info->or_index != 0 && --info->left_brackets_after_or == 0)
      {
        info->or_end_index = info->sql_byte_count;
        stat_or_token_cmp(info);
      }
      break; 
    }
    case '(':
    {
      /* if or found, we must find the right value of or,
         maybe it's a multi-value, so we must store the  '('
      */
      if (info->or_index != 0)
        info->left_brackets_after_or++;
      store_token(info, token);
      break;
    }
    case IDENT:
    case IDENT_QUOTED:
    {
      /*ident quoted such as the table name, column name and so on */
      LEX_YYSTYPE *lex_token = (LEX_YYSTYPE*)yylval;
      char *yytext = lex_token->lex_str.str;
      int yylen = lex_token->lex_str.length;
      store_token_identifier(info, token, yylen, yytext);
      info->last_id_index = info->sql_byte_count;
      break;
    }
    case OR_SYM:
    {
      /* or token found, we must store the right condition start position */
      info->left_brackets_after_or = 0;
      info->or_end_index = 0;
      info->or_index = info->sql_byte_count;
      store_token(info,token);
      break;
    }
    default:
    {
      store_token(info, token);
      break;
    }
  }
  DBUG_VOID_RETURN;
}

static void compute_query_digest(char digest[], unsigned char *query, int length)
{
  unsigned char md5[MD5_SIZE];
  compute_md5_hash((char *)md5, (const char *)query, length);
  array_to_hex((char *) digest, md5, MD5_SIZE);
}

static void start_table_stat(THD *thd)
{
  TABLE_LIST *table;
  if ((thd->lex->sql_command != SQLCOM_SELECT &&
       thd->lex->sql_command != SQLCOM_INSERT &&
       thd->lex->sql_command != SQLCOM_UPDATE &&
       thd->lex->sql_command != SQLCOM_DELETE) ||
       statistics_plugin_status == FALSE)
    return;
  TABLE_LIST *all_tables = thd->lex->query_tables;
  const char *db_name = NULL, *table_name = NULL;
  for (table = all_tables; table; table = table->next_global)
  {
    db_name = table->db;
    table_name = table->table_name;
    if (is_exclude_database(db_name)) continue;
    my_atomic_add64(&status_variables.table_enter_counts, 1);
    //thread_safe_increment(status_variables.table_enter_counts, &status_variables.status_lock);
    Table_Info *info = (Table_Info *)my_malloc(key_memory_top_sql, sizeof(Table_Info), MYF(MY_WME));
    sprintf(info->table_name, "%s.%s", db_name, table_name);
    info->command = thd->lex->sql_command;
    TableStats *hash_t = NULL;
    mysql_mutex_lock(&hash_table_lock);
    if ((hash_t = (TableStats *)my_hash_search(&stat_table_hash, (uchar*)info->table_name, strlen(info->table_name))) != NULL)
    {
      *hash_t += *info;
    }
    else
    {
      hash_t = new TableStats(*info);
      my_hash_insert(&stat_table_hash, (uchar*)hash_t);
    }
    mysql_mutex_unlock(&hash_table_lock);
    my_free(info);
  }
}

static void start_sql_stat(THD *thd)
{
  SQLInfo *info = thd->m_sql_info;
  if (info->is_stopped || info->is_full || info->exclude || statistics_plugin_status == 0)
  {
    if (info->exclude)
    {
      my_atomic_add64(&status_variables.discard_because_exclude, 1);
      //thread_safe_increment(status_variables.discard_because_exclude, &status_variables.status_lock);
    }
    if (info->is_full)
    {
      my_atomic_add64(&status_variables.discard_because_too_long, 1);
      //thread_safe_increment(status_variables.discard_because_too_long, &status_variables.status_lock);
      sql_print_warning("too long sql statement: %s", thd->query());
    }
    return;
  }
  Statistics *s = new Statistics(info);
  TABLE_LIST *table = thd->lex->query_tables;
  s->m_exec_times = GET_QUERY_EXEC_TIME(info);
  s->m_max_exec_times = s->m_min_exec_times = s->m_exec_times;
  Statistics *hash_s = NULL;
  my_atomic_add64(&status_variables.sql_enter_counts, 1);
  //thread_safe_increment(status_variables.sql_enter_counts, &status_variables.status_lock);
  mysql_mutex_lock(&hash_sql_lock);
  // if find in the hash table, do the + operator, if not insert into hash table
  if (table != NULL && table->db != NULL)
  {
	  memcpy(s->m_sql_format + s->sql_text_length, table->db, strlen(table->db));
	  s->length += strlen(table->db);
  }
  if ((hash_s = (Statistics *)my_hash_search(&stat_sql_hash, (uchar*)s->m_sql_format,s->length)) != NULL)
  {
    *hash_s += *s;
    delete s;
  }
  else
  {
    if (stat_sql_hash.records < statistics_max_sql_count)
    {
      my_hash_insert(&stat_sql_hash, (uchar*)s);
      statistics_array.array[statistics_array.length] = s;
      statistics_array.length++;
    }
    else
    {
       // if reach the max size, ignore it 
      status_variables.discard_because_hashtable_full++;
      delete s;
    }
  }
  mysql_mutex_unlock(&hash_sql_lock);
}

static void remove_expire_stats(THD *thd)
{
  TABLE *table;
  TABLE_LIST tables[2];
  READ_RECORD read_record_info;;
  ulonglong current_time = my_time(0);

  tables[0].init_one_table(C_STRING_WITH_LEN("mysql"),
                           C_STRING_WITH_LEN("sql_stats"),
                           "sql_stats", TL_WRITE_CONCURRENT_INSERT);
  tables[1].init_one_table(C_STRING_WITH_LEN("mysql"),
                           C_STRING_WITH_LEN("table_stats"),
                           "table_stats", TL_WRITE_CONCURRENT_INSERT);
  tables[0].next_global = tables[0].next_global = tables + 1;
  if (open_and_lock_tables(thd, tables, MYSQL_LOCK_IGNORE_TIMEOUT))
  {
    if (thd->get_stmt_da()->is_error())
    {
      sql_print_error("Fatal error: Can't open and lock privilege tables: %s", thd->get_stmt_da()->message_text());
      close_mysql_tables(thd);
      return;
    }
  }
  for (int i = 0; i < 2; i++)
  {
    if (tables[i].table)
    {
      init_read_record(&read_record_info, thd, table=tables[i].table, NULL, 1, 0, FALSE);
      table->use_all_columns();
      while (!(read_record_info.read_record(&read_record_info)))
      {
        char *ptr = get_field(thd->mem_root, table->field[0]);
        ulonglong start_time = strtoull(ptr, NULL, 10);
        if (current_time - start_time > statistics_expire_duration * SECONDS_PER_DAY)
        {
          table->file->ha_delete_row(table->record[0]);
        }
      }
      end_read_record(&read_record_info);
    }
  }
  close_mysql_tables(thd);
  status_variables.clean_counts++;
}

static void store_sql_stats(THD *thd)
{
  TABLE *table;
  char *buffer = NULL;
  TABLE_LIST tables;
  tables.init_one_table(C_STRING_WITH_LEN("mysql"),
                        C_STRING_WITH_LEN("sql_stats"),
                        "sql_stats", TL_WRITE_CONCURRENT_INSERT);

  if(open_and_lock_tables(thd, &tables, MYSQL_LOCK_IGNORE_TIMEOUT))
  {
    if (thd->get_stmt_da()->is_error())
    {
      sql_print_error("Fatal error: Can't open and lock privilege tables: %s",thd->get_stmt_da()->message_text());
      close_mysql_tables(thd);
      return;
    }
  }
  if ((table = tables.table) == NULL || table->file->ha_rnd_init(0))
  {
    close_mysql_tables(thd);
    return;
  }

  table->use_all_columns();
  buffer = (char*)my_malloc(key_memory_top_sql, MAX_SQL_TEXT_SIZE, MYF(MY_WME));
  mysql_mutex_lock(&hash_sql_lock);
  for (uint i = 0; i < statistics_array.length; i++)
  {
	  char dbname[MAX_DBNAME_SIZE] = { 0 };
    Statistics *s = statistics_array.array[i];
    if (s == NULL) continue;
    memset(buffer, 0, MAX_SQL_TEXT_SIZE);
    table->field[0]->store(start_output_time, TRUE);
    table->field[1]->store(end_output_time, FALSE);
    get_sql_text(buffer,dbname, s);
    table->field[2]->store(buffer, strlen(buffer), &my_charset_bin);
	  table->field[3]->store(dbname, strlen(dbname), &my_charset_bin);
    memset(buffer, 0, MAX_SQL_TEXT_SIZE);
    compute_query_digest(buffer, s->m_sql_format, s->length);
    table->field[4]->store(buffer, strlen(buffer), &my_charset_bin);
    memset(buffer, 0, MAX_SQL_TEXT_SIZE);
    void *index = s->m_index_queue.new_iterator();
    char *p = buffer;
    while(index != NULL)
    {
      INDEX_INFO *info = s->m_index_queue.iterator_value(index);
      /* if more indexes exist, maybe cause the buffer out of memory */
      if (buffer + MAX_SQL_TEXT_SIZE - p < info->index_name.length + 10)
      { 
        sprintf(p, "....");
        p += 4;
        break;
      } 
      sprintf(p, "%s(%d),", info->index_name.str, info->index_reads);
      while(*p != '\0') p++;
      index = s->m_index_queue.iterator_next(index);
    }
    if (p != buffer)
    {
      p--; *p = '\0';
    }
    else
    {
      sprintf(buffer, "NULL");
    }
    table->field[5]->store(buffer, strlen(buffer), &my_charset_bin);
    table->field[6]->store(s->m_memory_temp_table_created, TRUE);
    table->field[7]->store(s->m_disk_temp_table_created, TRUE);
    table->field[8]->store(s->m_row_reads, TRUE);
    table->field[9]->store(s->m_byte_reads, TRUE);
    table->field[10]->store(s->m_max_exec_times,  TRUE);
    table->field[11]->store(s->m_min_exec_times, TRUE);
    table->field[12]->store(s->m_exec_times, TRUE);
    table->field[13]->store(s->m_exec_count, TRUE);

    table->field[0]->set_notnull();
    table->field[1]->set_notnull();
    table->field[2]->set_notnull();
    table->field[3]->set_notnull();
    table->field[4]->set_notnull();
    table->field[5]->set_notnull();
    table->field[6]->set_notnull();
    table->field[7]->set_notnull();
    table->field[8]->set_notnull();
    table->field[9]->set_notnull();
    table->field[10]->set_notnull();
    table->field[11]->set_notnull();
    table->field[12]->set_notnull();
	  table->field[13]->set_notnull();
    if (table->file->ha_write_row(table->record[0]))
      break;
  }
  my_free(buffer);
  my_hash_reset(&stat_sql_hash);
  statistics_array.length = 0;
  mysql_mutex_unlock(&hash_sql_lock);
  table->file->ha_rnd_end();
  close_mysql_tables(thd);
}

static void store_table_stats(THD *thd)
{
  TABLE *table;
  TABLE_LIST tables;

  tables.init_one_table(C_STRING_WITH_LEN("mysql"),
                        C_STRING_WITH_LEN("table_stats"),
                        "table_stats",TL_WRITE_CONCURRENT_INSERT);

  if(open_and_lock_tables(thd, &tables, MYSQL_LOCK_IGNORE_TIMEOUT))
  {
    if (thd->get_stmt_da()->is_error())
    {
      sql_print_error("Fatal error: Can't open and lock privilege tables: %s",thd->get_stmt_da()->message_text());
      close_mysql_tables(thd);
      return;
    }
  }
  if ((table = tables.table) == NULL || table->file->ha_rnd_init(0))
  {
    close_mysql_tables(thd);
    return;
  }

  table->use_all_columns();
  mysql_mutex_lock(&hash_table_lock);
  for(uint i = 0; i < stat_table_hash.records; i++)
  {
    char buffer[NAME_CHAR_LEN] = {0};
    TableStats *ts = (TableStats*)my_hash_element(&stat_table_hash, i);
    if (ts == NULL) continue;
    table->field[0]->store(start_output_time, TRUE);
    table->field[1]->store(end_output_time, TRUE);
    const char *p = ts->table_name;
    while(p != NULL && *p != '.') p++;
    strncpy(buffer, ts->table_name, (p - ts->table_name));
    table->field[2]->store(buffer,strlen(buffer),&my_charset_bin);
    strcpy(buffer, p + 1);
    table->field[3]->store(buffer, strlen(buffer), &my_charset_bin);
    table->field[4]->store(ts->select_count, TRUE);
    table->field[5]->store(ts->insert_count, TRUE);
    table->field[6]->store(ts->update_count, TRUE);
    table->field[7]->store(ts->delete_count, TRUE);

    table->field[0]->set_notnull();
    table->field[1]->set_notnull();
    table->field[2]->set_notnull();
    table->field[3]->set_notnull();
    table->field[4]->set_notnull();
    table->field[5]->set_notnull();
    table->field[6]->set_notnull();
    table->field[7]->set_notnull();

    if(table->file->ha_write_row(table->record[0]))
      break;
  }
  my_hash_reset(&stat_table_hash);
  mysql_mutex_unlock(&hash_table_lock);
  table->file->ha_rnd_end();
  close_mysql_tables(thd);
}

extern "C" void *statistics_output_thread(void *arg)
{
  THD *thd;
  my_thread_init();
  if(! (thd = new THD)) return NULL;
  thd->variables.option_bits &= ~OPTION_BIN_LOG;
  thd->thread_stack = (char*)&thd;
  thd->store_globals();
  thd->set_new_thread_id();
  Global_THD_manager::get_instance()->add_thd(thd);
  output_thread_running = TRUE;

  while(output_thread_running)
  {
    if (statistics_plugin_status)
    {
      struct timespec abstime;
      set_timespec(&abstime, statistics_output_cycle * SECONDS_PER_HOUR);
      mysql_mutex_lock(&stat_output_lock);
      start_output_time = my_time(0);
      mysql_cond_timedwait(&stat_output_cond, &stat_output_lock, &abstime);
      end_output_time = my_time(0);
      status_variables.output_counts++;
      mysql_mutex_unlock(&stat_output_lock);
      if (opt_readonly)
      {
        mysql_mutex_lock(&hash_sql_lock);
        my_hash_reset(&stat_sql_hash);
        statistics_array.length = 0;
        mysql_mutex_unlock(&hash_sql_lock);
      
        mysql_mutex_lock(&hash_table_lock);
        my_hash_reset(&stat_table_hash);
        mysql_mutex_unlock(&hash_table_lock);
      }
      else
      {
        store_sql_stats(thd);
        store_table_stats(thd);
        remove_expire_stats(thd);
      }
    }
    else
    {
      my_sleep(500000);
    }
  }
  Global_THD_manager::get_instance()->remove_thd(thd);
  thd->release_resources(); 
  delete thd;
  output_thread_exit = TRUE;
  my_thread_end();
  my_thread_exit(0);
  return NULL;
}

void statistics_exclude_current_sql(THD *thd)
{
  if (thd->m_sql_info == NULL || thd->m_sql_info->is_stopped) return;
  LEX *lex = thd->lex;
  if (is_exclude_sql(sql_command[lex->sql_command])
      || contain_exclude_database(lex->query_tables)
      || lex->describe)
  {
    thd->m_sql_info->exclude = TRUE;
  }
}

SQLInfo *statistics_create_sql_info(uint charset)
{
  DBUG_ENTER("statistics create sql info");
  SQLInfo *info = new SQLInfo();
  if (info != NULL)
  {
    info->sql_format = (unsigned char *)my_malloc(key_memory_top_sql, MAX_SQL_TEXT_SIZE, MYF(MY_WME));
    memset(info->sql_format, 0 , MAX_SQL_TEXT_SIZE);
    info->charset_number = charset;
  }
  DBUG_RETURN(info);
}

void statistics_destory_sql_info(SQLInfo *info)
{
  DBUG_ENTER("statistics destory sql info");
  if (info != NULL)
  {
    if (info->sql_format != NULL)
    {
      my_free(info->sql_format);
      info->sql_format = NULL;
    }
    delete info;
  }
  DBUG_VOID_RETURN;
}

void statistics_reset_sql_info(SQLInfo *info, uint charset)
{
  DBUG_ENTER("stat reset sql info");
  info->is_full = false;
  info->exclude = false;
  info->disk_temp_table_created = 0;
  info->last_id_index = 0;
  info->logical_reads = 0;
  info->memory_temp_table_created = 0;
  info->row_reads = 0;
  info->byte_reads = 0;
  info->sql_byte_count = 0;
  info->start_time = 0;
  info->charset_number = charset;
  INDEX_INFO *index = NULL;
  while((index = info->index_queue.pop()) != NULL)
  {
    my_free(index->index_name.str);
    my_free(index);
  }
  memset(info->sql_format,0,MAX_SQL_TEXT_SIZE);
  DBUG_VOID_RETURN;
}

bool statistics_show_sql_stats(uint counts, THD *thd)
{
  List<Item> field_list;
  Protocol *protocol = thd->get_protocol();
  Statistics *s;
  DBUG_ENTER("stat show sql stats");
  field_list.push_back(new Item_empty_string("SQL_TEXT", MAX_SQL_TEXT_SIZE));
  field_list.push_back(new Item_empty_string("DB_NAME", MAX_DBNAME_SIZE));
  field_list.push_back(new Item_empty_string("INDEX", MAX_SQL_TEXT_SIZE));
  field_list.push_back(new Item_int(NAME_STRING("MEMORY_TEMP_TABLES"), 0, MY_INT32_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("DISK_TEMP_TABLES"), 0, MY_INT32_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("ROW_READS"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("BYTE_READS"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("MAX_EXEC_TIMES"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("MIN_EXEC_TIMES"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("EXEC_TIMES"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("EXEC_COUNT"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  if (thd->send_result_metadata(&field_list, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(TRUE);
  mysql_mutex_lock(&hash_sql_lock);
  statistics_sort_by_exectime(statistics_array.array, statistics_array.length);
  char *buffer = (char*)my_malloc(key_memory_top_sql, MAX_SQL_TEXT_SIZE, MYF(MY_WME));
  for (uint i = 0; i < statistics_array.length && i < counts; i++)
  {
	char dbname[MAX_DBNAME_SIZE] = { 0 };
    s = statistics_array.array[i];
    if (s == NULL)continue;
    memset(buffer, 0, MAX_SQL_TEXT_SIZE);
    protocol->start_row();
    get_sql_text(buffer, dbname, s);
    protocol->store(buffer, system_charset_info);
	protocol->store(dbname, system_charset_info);
    void *index = s->m_index_queue.new_iterator();
    char *p = buffer;
    while (index != NULL)
    {
      INDEX_INFO *info = s->m_index_queue.iterator_value(index);
      sprintf(p, "%s(%d),", info->index_name.str, info->index_reads);
      while (*p != '\0') p++;
      index = s->m_index_queue.iterator_next(index);
    }
    if (p != buffer)
    {
      p--; *p = '\0';
    }
    else
    {
      sprintf(buffer, "NULL");
    }
    protocol->store(buffer, system_charset_info);
    protocol->store((ulonglong)s->m_memory_temp_table_created);
    protocol->store((ulonglong)s->m_disk_temp_table_created);
    protocol->store((ulonglong)s->m_row_reads);
    protocol->store((ulonglong)s->m_byte_reads);
    protocol->store((ulonglong)s->m_max_exec_times);
    protocol->store((ulonglong)s->m_min_exec_times);
    protocol->store((ulonglong)s->m_exec_times);
    protocol->store((ulonglong)s->m_exec_count);
    if (protocol->end_row())
    {
      mysql_mutex_unlock(&hash_sql_lock);
      DBUG_RETURN(TRUE);
    }
  }
  mysql_mutex_unlock(&hash_sql_lock);
  my_free(buffer);
  my_eof(thd);
  DBUG_RETURN(FALSE);
}

bool statistics_show_table_stats(THD *thd)
{
  List<Item> field_list;
  Protocol *protocol = thd->get_protocol();
  DBUG_ENTER("stat show table stats");
  field_list.push_back(new Item_empty_string("DBNAME", NAME_CHAR_LEN));
  field_list.push_back(new Item_empty_string("TABLE_NAME",NAME_CHAR_LEN));
  field_list.push_back(new Item_int(NAME_STRING("SELECT_COUNT"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("INSERT_COUNT"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("UPDATE_COUNT"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int(NAME_STRING("DELETE_COUNT"), 0, MY_INT64_NUM_DECIMAL_DIGITS));
  if (thd->send_result_metadata(&field_list, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(TRUE);
  mysql_mutex_lock(&hash_table_lock);
  for(uint i = 0; i < stat_table_hash.records; i++)
  {
    char buffer[NAME_CHAR_LEN] = {0};
    TableStats *ts = (TableStats *)my_hash_element(&stat_table_hash, i);
    if (ts == NULL) continue;
    protocol->start_row();
    const char *p = ts->table_name;
    while(p != NULL && *p != '.') p++;
    strncpy(buffer, ts->table_name, (p - ts->table_name));
    protocol->store(buffer,system_charset_info);
    strcpy(buffer, p + 1);
    protocol->store(buffer,system_charset_info);
    protocol->store((ulonglong)ts->select_count);
    protocol->store((ulonglong)ts->insert_count);
    protocol->store((ulonglong)ts->update_count);
    protocol->store((ulonglong)ts->delete_count);
    if (protocol->end_row())
    {
      mysql_mutex_unlock(&hash_table_lock);
      DBUG_RETURN(TRUE);
    }
  }
  mysql_mutex_unlock(&hash_table_lock);
  my_eof(thd);
  DBUG_RETURN(FALSE);
}

bool statistics_show_status(THD *thd)
{
  return FALSE;
}

void statistics_end_sql_statement(THD *thd)
{
  if (thd->is_a_srv_session() || thd->m_sql_info == NULL || thd->m_sql_info->sql_byte_count == 0 || thd->is_error()) return;
  DBUG_ENTER("stat end sql statement");
  start_table_stat(thd);
  start_sql_stat(thd);
  DBUG_VOID_RETURN;
}

void statistics_start_sql_statement(THD *thd)
{
  INIT_START_TIME(thd);
}

void statistics_save_index(JOIN *join)
{
  THD *thd = join->thd;
  DBUG_ENTER("stat save index");
  if (thd->is_a_srv_session() || thd->m_sql_info == NULL || thd->m_sql_info->is_full || thd->m_sql_info->is_stopped || thd->m_sql_info->sql_byte_count == 0)
    DBUG_VOID_RETURN;
  for(uint i = 0; i < join->tables; i++)
  {
	  //JOIN_TAB *tab = join->join_tab + i; 
	QEP_TAB *tab = join->qep_tab + i;

    /*if do the select count(*) from myisam_table tab will be null*/
    if (tab == NULL) continue;
    TABLE *table=tab->table();
    char buffer[512] = {0};
    INDEX_INFO *index = (INDEX_INFO *)my_malloc(key_memory_top_sql, sizeof(INDEX_INFO),MYF(MY_WME));
    if (tab->ref().key_parts)
    {
      KEY *key_info = table->key_info + tab->ref().key;
      sprintf(buffer, "%s.%s", table->alias, key_info->name);
      index->index_name.str = my_strdup(key_memory_top_sql, buffer,MYF(MY_WME));
      index->index_name.length = strlen(buffer);
      index->index_reads = 1;
      thd->m_sql_info->index_queue.push_back(index);
    }
    else if(tab->type() == JT_INDEX_SCAN)
    {
      KEY *key_info = table->key_info + tab->index();
      sprintf(buffer, "%s.%s", table->alias, key_info->name);
      index->index_name.str = my_strdup(key_memory_top_sql, buffer,MYF(MY_WME));
      index->index_name.length = strlen(buffer);
      index->index_reads = 1;
      thd->m_sql_info->index_queue.push_back(index);
    }
    else if(tab->quick())
    {
      StringBuffer<512> tmp1(system_charset_info);
      StringBuffer<512> tmp2(system_charset_info);
      tab->quick()->add_keys_and_lengths(&tmp1, &tmp2);
      sprintf(buffer, "%s.%s", table->alias, tmp1.c_ptr());
      index->index_name.str = my_strdup(key_memory_top_sql, buffer,MYF(MY_WME));
      index->index_name.length = strlen(buffer);
      index->index_reads = 1;
      thd->m_sql_info->index_queue.push_back(index);
    }
    else
    {
      my_free(index);
    }
  }
  DBUG_VOID_RETURN;
}

void statistics_save_index(THD *thd,  TABLE *table , Modification_plan *plan)
{
  if (thd->is_a_srv_session())
    return;
  if ( plan->tab && plan->tab->quick())
  {
    char buffer[512] = {0};
    INDEX_INFO *index = (INDEX_INFO *)my_malloc(key_memory_top_sql, sizeof(INDEX_INFO),MYF(MY_WME));
    StringBuffer<512> str_key(system_charset_info);
    StringBuffer<512> str_key_len(system_charset_info);
    plan->tab->quick()->add_keys_and_lengths(&str_key, &str_key_len);
    sprintf(buffer, "%s.%s", table->alias, str_key.c_ptr());
    index->index_name.str = my_strdup(key_memory_top_sql, buffer, MYF(MY_WME));
    index->index_name.length = strlen(buffer);
    index->index_reads = 1;
    thd->m_sql_info->index_queue.push_back(index);
  }
}

int statistics_init(void)
{
  my_thread_handle hThread;
  mysql_mutex_init(0, &stat_output_lock, MY_MUTEX_INIT_FAST);
  mysql_cond_init(0, &stat_output_cond);
  mysql_mutex_init(0, &status_variables.status_lock, MY_MUTEX_INIT_FAST);
  if (opt_readonly)
  {
    sql_print_information("start with --read-only, the stats info will not to be outputted");
  }
  statistics_exclude_db_queue.init();
  statistics_exclude_sql_queue.init();

  init_statistics_hash();
  update_exclude_db_list();
  update_exclude_sql_list();
  mysql_thread_create(0, &hThread, NULL, statistics_output_thread, NULL);
  return 0;
}

int statistics_deinit(void)
{
  if (output_thread_running)
  {
    output_thread_running = FALSE;
    mysql_cond_signal(&stat_output_cond);
    while (!output_thread_exit) //wait until all the topsql infos outputted
    {
      sleep(1);
    }
  }
  destory_statistics_hash();
  mysql_mutex_destroy(&stat_output_lock);
  mysql_cond_destroy(&stat_output_cond);
  mysql_mutex_destroy(&status_variables.status_lock);
  return 0;
}
