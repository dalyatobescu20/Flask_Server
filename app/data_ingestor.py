"""Module for defining and managing data ingestor."""
import pandas as pd

from app.task_runner import convert_to_serializable, flatten_nested_dict, logger


class DataIngestor:
    """
    Class for loading and processing data from a CSV file.
    """

    def __init__(self, csv_path: str):
        """
        Initialize the DataIngestor with the path to the CSV file.
        """
        self.csv_path = csv_path
        self.data = None
        self.questions = None
        self.load_data()

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical '
            'activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical '
            'activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in '
            'muscle-strengthening activities on 2 or more days a week',
            'Percent     of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical '
            'activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]

    def load_data(self):
        """
        Load data from the CSV file.
        """
        try:
            self.data = pd.read_csv(self.csv_path)
            self.questions = self.data['Question'].unique()
        except FileNotFoundError:
            logger.info(f"File not found: {self.csv_path}")

    def state_mean(self, state: str, question: str):
        """
        Calculate the mean value for the specified state and question.
        """
        if question not in self.questions:
            return None

        state_data = self.data[(self.data['LocationDesc'] == state) & (self.data['Question'] == question)]
        state_data_sorted = state_data.sort_values(by='Data_Value')
        mean_value = state_data_sorted['Data_Value'].mean()
        return pd.Series({state: mean_value})

    def states_mean(self, question: str):
        """
        Calculate the mean value for each state for the specified question.
        """
        if question not in self.questions:
            return None
        question_data = self.data[self.data['Question'] == question]
        mean_values = question_data.groupby('LocationDesc')['Data_Value'].mean()
        mean_values = mean_values.sort_values(ascending=True)
        return mean_values

    def best5(self, question: str):
        """
        Get the top 5 states with the best values for the specified question.
        """
        if question not in self.questions:
            return ValueError(f"Invalid question: {question}")
        question_data = self.data[self.data['Question'] == question]
        if question in self.questions_best_is_min:
            mean_values = question_data.groupby('LocationDesc')['Data_Value'].mean()
            mean_values = mean_values.sort_values(ascending=True).head(5)
        else:
            mean_values = question_data.groupby('LocationDesc')['Data_Value'].mean()
            mean_values = mean_values.sort_values(ascending=False).head(5)
        return mean_values

    def worst5(self, question: str):
        """
        Get the top 5 states with the worst values for the specified question.
        """
        if question not in self.questions:
            return ValueError(f"Invalid question: {question}")
        question_data = self.data[self.data['Question'] == question]
        if question in self.questions_best_is_min:
            mean_values = question_data.groupby('LocationDesc')['Data_Value'].mean()
            mean_values = mean_values.sort_values(ascending=False).head(5)
        else:
            mean_values = question_data.groupby('LocationDesc')['Data_Value'].mean()
            mean_values = mean_values.sort_values(ascending=True).head(5)
        return mean_values

    def global_mean(self, question: str):
        """
        Calculate the global mean value for the specified question.
        """
        if question not in self.questions:
            return None
        question_data = self.data[self.data['Question'] == question]
        mean_value = question_data['Data_Value'].mean()
        return pd.Series({"global_mean": mean_value})

    def diff_from_mean(self, question: str):
        """
        Calculate the difference from the global mean for each state for the specified question.
        """
        if question not in self.questions:
            return ValueError(f"Invalid question: {question}")

        global_mean = self.global_mean(question)
        global_mean = global_mean['global_mean']
        state_diffs = {}
        states = self.states_mean(question)
        for state in states.index:
            state_mean = states[state]
            if state_mean is not None:
                # Calculate the difference from the global mean
                diff = global_mean - state_mean
                # Store the difference in the state_diffs dictionary
                state_diffs[state] = diff
        return state_diffs

    def state_diff_from_mean(self, state: str, question: str):
        """
        Calculate the difference from the global mean for the specified state and question.

        """
        if question not in self.questions:
            return ValueError(f"Invalid question: {question}")

        # Calculate global mean for the specified question
        global_mean = self.global_mean(question)
        global_mean = global_mean['global_mean']
        # Calculate the mean for the specified state and question
        state_mean = self.state_mean(state, question)
        diff = global_mean - state_mean
        result = {state: diff}
        result = convert_to_serializable(result)
        result = flatten_nested_dict(result)

        return result

    def mean_by_category(self, question: str):
        """
        Calculate the mean value for each state by category and segment for the specified question.
        """
        if question not in self.questions:
            return None
        mean_values = {}
        # Filter data for the specified question and iterate over unique states
        for state in self.data[self.data['Question'] == question]['LocationDesc'].unique():
            state_data = self.data[(self.data['LocationDesc'] == state) & (self.data['Question'] == question)]
            # Group by both StratificationCategory1 and Stratification1, then calculate mean
            grouped_data = state_data.groupby(['StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
            # Iterate over the index (category, segment) and mean values
            for idx, mean_value in grouped_data.items():
                category, segment = idx
                key = (state, category, segment)
                mean_values[key] = mean_value
        mean_values = convert_tuple_keys_to_str(mean_values)
        # sort alphabetically
        mean_values = dict(sorted(mean_values.items(), key=lambda x: x[0]))
        # Return the flattened dictionary containing mean values by category and segment for each state
        return pd.Series(mean_values)

    def state_mean_by_category(self, state: str, question: str):
        """
        Calculate the mean value by category and segment for the specified state and question.
        """
        if question not in self.questions:
            return None
        mean_values = {}
        # Filter data for the specified question and state
        state_data = self.data[(self.data['LocationDesc'] == state) & (self.data['Question'] == question)]
        # Group by both StratificationCategory1 and Stratification1, then calculate mean
        grouped_data = state_data.groupby(['StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
        # Iterate over the index (category, segment) and mean values
        for idx, mean_value in grouped_data.items():
            category, segment = idx
            key = (category, segment)
            mean_values[key] = mean_value
        mean_result = {state: mean_values}
        mean_values = convert_tuple_keys_to_str(mean_result)
        # Return the flattened dictionary containing mean values by category and segment for the specified state
        return pd.Series(mean_values)


def convert_tuple_keys_to_str(dictionary):
    """
    Convert tuple keys in a dictionary to strings.
    """
    new_dict = {}
    for key, value in dictionary.items():
        if isinstance(key, tuple):
            new_key = str(key)
        else:
            new_key = key
        if isinstance(value, dict):
            new_value = convert_tuple_keys_to_str(value)
        else:
            new_value = value
        new_dict[new_key] = new_value

    return new_dict
