# UN Data Krunch

unegocio's awesome data processing tool...

## Easy run

> python main.py -f [gin.config file path]

## Main modules

The process pipeline is broken down to the following components:

#### Data Source

Data import module that sources the advertise data. This can be
local files or Datastore bucket.

#### Parser

In case of raw data, the Parser module must be configured to parse the raw structure
into a python friendly data structure (i.e. dictionary).

#### Cleaner and Validation

The Cleaner is the pipe that encompass all process required to transform provided parser's 
data into a formatted advertise schema that is validated by the Validation module.

#### Encoder

The Encoder is the main pipeline that takes valid/clean data and encode it to be
used in a process (generate labelling maps or any other intermediate process) or
as training ready dataset.

**While most modules may remaing the same for most purposes, this module is model
specific and must be specified for every new model idea.**

#### Reducer [Optional]

While processing using multiple workers, some processes need to aggregate the results into
a unique file/source. In those cases, the Reducer module must be specified for a given aggregation
logic.


