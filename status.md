# Status

## Stable Api

[Reference][1]
| Command | Developed | Tested  | Notes |
| :---: | :---: | --- | :---: |
| abortTransaction | Not developed |  |  |
| aggregate | :heavy_check_mark: |  :heavy_check_mark:  | |
| bulkWrite() | Not developed |  |  |
| collMod | Not developed |  |  |
| commitTransaction | Not developed |  |  |
| count | Not developed |  |  |
| create | Not developed |  |  |
| createIndexes | Not developed |  |  |
| delete | Not developed |  |  |
| drop | Not developed |  |  |
| dropDatabase | Not developed |  |  |
| dropIndexes | Not developed |  |  |
| endSessions | Not developed |  |  |
| explain | Not developed |  | |
| find | Not developed |  |  |
| findAndModify | Not developed |  |  |
| getMore | Not developed |  |  |
| insert | Not developed |  |  |
| hello | Not developed |  |  |
| killCursor | Not developed |  |  |
| listCollections | Not developed |  |  |
| listDatabases | Not developed |  |  |
| listIndexes | Not developed |  |  |
| ping | Not developed |  |  |
| refreshSession | Not developed |  |  |
| update | Not developed |  |  |

## Session

> :warning: **Warning:** Sessions are available only for Replica sets and Sharded Clusters

[Reference][4]
| Topic | Developed | Tested  | Notes |
| :---: | :---: | --- | :---: |
| Server Session | :heavy_check_mark: |  |  |
| causalConsistency |:white_square_button: |  |  |
| retryableWrites | :white_square_button: |  |  |

| Command | Developed | Tested  | Notes |
| :---: | :---: | --- | :---: |
| abortTransaction | :heavy_check_mark: |  |  |
| commitTransaction |:heavy_check_mark: |  |  |
| endSessions | :white_square_button: |  |  |
| killAllSessions | :white_square_button: |  |  |
| killAllSessionsByPattern | :white_square_button: |  |  |
| killSessions |:white_square_button: |  |  |
| refreshSessions | :white_square_button: |  |  |
| startSession | :heavy_check_mark: |  |  |

## Commands

[Reference][5]
| Command | Developed | Tested  | Notes |
| :---: | :---: | --- | :---: |
| | **Aggregation**  | **Commands** | |
| aggregate | :heavy_check_mark: |  |  |
| count |:heavy_check_mark: |  |  |
| distinct | :heavy_check_mark: |  |  |
| mapReduce | :white_square_button: |  |  |

 [1]: https://www.mongodb.com/docs/manual/reference/stable-api-changelog/#std-label-stable-api-changelog
 [4]: https://www.mongodb.com/docs/manual/reference/command/nav-sessions/
 [5]: https://www.mongodb.com/docs/manual/reference/command/
