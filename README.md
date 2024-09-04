# Nutrition and Physical Activity in the USA: A Multi-Threaded Statistics Server

This project is a **multi-threaded server** developed in **Python** using the **Flask** framework. The server processes a dataset containing health-related information (nutrition, physical activity, and obesity) in the United States between 2011 and 2022. The data is provided by the U.S. Department of Health & Human Services. The application allows you to perform various statistical calculations, such as averages and comparisons, based on state data.

## Table of Contents

- [Objective](#objective)
- [Features](#features)
- [Dataset Structure](#dataset-structure)
- [Available Endpoints](#available-endpoints)
  - [Get State-Wise Mean](#apistates_mean)
  - [Get Mean for a Specific State](#apistate_mean)
  - [Top 5 Best States](#apibest5)
  - [Top 5 Worst States](#apiworst5)
  - [Global Mean](#apiglobal_mean)
  - [Difference from Global Mean](#apistate_diff_from_mean)
  - [Mean by Category](#apimean_by_category)
  - [Graceful Shutdown](#apigraceful_shutdown)
  - [Check Job Status](#apijobs)
  - [Number of Remaining Jobs](#apinum_jobs)
  - [Get Job Results](#apiget_resultsjob_id)
- [Installation and Execution](#installation-and-execution)
- [Testing](#testing)

## Objective

- Efficiently use synchronization elements studied in labs.
- Implement a concurrent application using the client-server model.
- Deepen Python knowledge (syntax, classes, threads, synchronization, and using Python modules for multi-threading).

## Features

- **Flask Multi-Threaded Server**: Asynchronous processing of jobs using a job queue and thread pool.
- **Statistical Calculations**: Generate averages and rankings based on health data from U.S. states between 2011-2022.
- **Support for Various Requests**: Endpoints to calculate specific statistics such as averages, rankings, and comparisons with global averages.
- **Logging**: Detailed logging with rotating log files.

## Dataset Structure

The dataset includes health-related data on the following topics:

- Percentage of adults who engage in no leisure-time physical activity.
- Percentage of adults aged 18 and older with obesity.
- Percentage of adults aged 18 and older with overweight classification.
- Percentage of adults who achieve at least 300 minutes of moderate aerobic activity or 150 minutes of vigorous activity per week.
- Percentage of adults who engage in aerobic activity and muscle-strengthening activities.
- Percentage of adults who consume fruits or vegetables less than once per day.

The statistics are calculated from the **Data_Value** column in the CSV file.

## Available Endpoints

### `/api/states_mean`
Calculates the mean **Data_Value** for each U.S. state from 2011 to 2022, based on a specific question. The results are sorted in ascending order.

### `/api/state_mean`
Receives a specific state and question, then calculates the mean **Data_Value** for that state from 2011 to 2022.

### `/api/best5`
Returns the top 5 states with the best averages for a specific question. What defines "best" depends on the context of the question (e.g., the lowest percentage for inactivity or the highest percentage for regular physical activity).

### `/api/worst5`
Returns the bottom 5 states with the worst averages for a specific question. The definition of "worst" varies based on the question.

### `/api/global_mean`
Calculates the global mean **Data_Value** from 2011 to 2022 for a specific question across all states.

### `/api/state_diff_from_mean`
Calculates the difference between the mean for a specific state and the global mean for a specific question.

### `/api/mean_by_category`
Calculates the mean **Data_Value** by category (e.g., age group, gender) for a specific question across all states.

### `/api/state_mean_by_category`
Receives a specific state and calculates the mean **Data_Value** by category for a specific question.

### `/api/graceful_shutdown`
Notifies the server to stop accepting new requests, processes any remaining jobs, and shuts down gracefully.

### `/api/jobs`
Returns a JSON object with the status of all jobs in the queue, in the following format:
```json
{
  "status": "done",
  "data": [
    { "job_id_1": "done" },
    { "job_id_2": "running" },
    { "job_id_3": "running" }
  ]
}
