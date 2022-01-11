
# Miner-XenForo

Miner-XenForo is a python tool developed to **mine, clean, anonymize, and create conversations** from online forums using [XenForo](https://xenforo.com/). The development was made following the methodology [MCCD](https://github.com/Ze-Carioca-Team/MCCD).


The entire tool is implemented in four different modules, described below.

## Modules


Terminology
 - **Thread:** A topic inside an online forum using XenForo
 - **Post:** A message replying to a specific topic

### Miner

This module is responsible for acquiring the data from the sources through web requests.


This module is executed using the `main.py` script.

Parameters
|Parameter| Required | Description | 
|--|--|--|
| -url | True  | The url from a data source using XenForo |
| -s   | False | Shows a summary using the available data |
| -rt  | False | Reload Threads |
| -rp  | False | Reload Posts |
| -cp  | False | Cache web response. Useful on debugging |
| -mr  | False | Maximum number of requests performed simultaneously. Caution to use as some sites may block request-intensive users |

### Anonymizer

This module is implemented in the file `anonymizer.py` and is the smallest module. This module will search the config folder during its execution and replace `member_href` and `member_name` with an integer. The same integer will be used every time the same user is found.

Parameters
|Parameter| Required | Description | 
|--|--|--|
| -url | True | The url from data source using XenForo |


**\*WARNING:** The original files will be replaced while this module is being executed.

### Cleaner & Identify Conversations

This module removes HTML tags and replaces them with pre-defined tags. Identifying conversation between one or more users is also executed in this module if specified to do so.

Parameters
|Parameter| Required | Description | 
|--|--|--|
| -url | True  | The url from a data source using XenForo |
| -min | False | Minimum size of a thread to be processed. |
| -max | False | Maximum size of a thread to be processed. |
| -c   | False | Identify conversations |
| -ca  | False | Use cache to speed up the process. It is recommended to be used as True |
| -t   | False | Number of parallel processing simultaneously. |
| -oem | False | Only process threads where conversations_lens==nan. Usefull to continue processing after change parameters |



**\*WARNING:** Forums with large threads or lots of replies may generate many data. Multiple files are generated per conversation, and the longest the conversation is, the larger the files will be.



## Ready datasets
  
There are already available datasets that were created using Miner-XenForo. More details and how to access them are available [here.](https://github.com/MatheusFerraroni/datasets_from_minerxenforo)