#ifndef IOSTAT_INCLUDED
#define IOSTAT_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif
	typedef enum
	{
		LOG_READ		= 0,
		PHY_READ
	}IO_TYPE;

	typedef void(*_io_stat_func_ptr)(IO_TYPE type);
	extern _io_stat_func_ptr io_stat_func_ptr;

	void set_io_stat_flag(const bool *flag);
	void thd_io_increased(IO_TYPE type);
#ifdef __cplusplus
}
#endif
#endif //IOSTAT_INCLUDED