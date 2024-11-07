light:
	mprof run python light_query.py
	mprof plot

heavy:
	mprof run python heavy_query.py || mprof plot