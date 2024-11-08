light:
	mprof run python light_query.py $(size)
	mprof plot

heavy:
	mprof run python heavy_query.py $(size)
	mprof plot 