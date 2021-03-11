# Spark Test Session

Attempt to spy on various parts of the Spark ecosystem in order to capture reads and writes.  A captured read would load from a map.  A capture write would save to the map.  Thus a test could store initial data in the map, run the code, then extract the result from the map, and compare against expected dataframe.

Unfortunately, once a read returns a spy dataframe, any transformation will result in a new non-spy dataframe.  Thus any capture of the write is lost.  I have not found any way around this so far.

It may be possible to spy on all instances of some class, like DataFrameWriter, using this library called PowerMock, but it is very much Java oriented, and couldn't figure out how to make it work for my purposes.
