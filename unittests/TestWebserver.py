import json
import unittest
from app import DataIngestor
from app.task_runner import convert_to_serializable


class TestWebserver(unittest.TestCase):
    def test_state_mean_request(self):
        question = {
            "question": "Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic "
                        "physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an "
                        "equivalent combination)",
            "state": "Guam"
        }
        # Initialize DataIngestor with appropriate data file path
        data_ingestor = DataIngestor("../nutrition_activity_obesity_usa_subset.csv")
        # Define output file path
        output_file_path = f"output_state_mean.json"
        # Write the output to the output file
        with open(output_file_path, 'w') as f:
            output = data_ingestor.state_mean(question["state"], question["question"])
            # convert the output to a serializable format
            output = convert_to_serializable(output)
            # write the output to the file
            json.dump(output, f)
        # Define the path to the expected result file
        result_file_path = f"../tests/state_mean/output/out-1.json"
        # Read the content of the output file
        with open(output_file_path, 'r') as f:
            output = f.read()
        # Read the content of the expected result file
        with open(result_file_path, 'r') as f:
            result = f.read()
        # Compare the output with the expected result
        self.assertEqual(output, result)

    def test_states_mean_request(self):
        question = {"question": "Percent of adults aged 18 years and older who have an overweight classification"}
        # Initialize DataIngestor with appropriate data file path
        data_ingestor = DataIngestor("../nutrition_activity_obesity_usa_subset.csv")
        # Define output file path
        output_file_path = f"output_states_mean.json"
        # Write the output to the output file
        with open(output_file_path, 'w') as f:
            output = data_ingestor.states_mean(question["question"])
            # convert the output to a serializable format
            output = convert_to_serializable(output)
            # write the output to the file
            json.dump(output, f)
        # Define the path to the expected result file
        result_file_path = f"../tests/states_mean/output/out-1.json"
        # Read the content of the output file
        with open(output_file_path, 'r') as f:
            output = f.read()
        # Read the content of the expected result file
        with open(result_file_path, 'r') as f:
            result = f.read()
        # Compare the output with the expected result
        self.assertEqual(output, result)

    def test_best5_request(self):
        question = {"question": "Percent of adults aged 18 years and older who have an overweight classification"}
        # Initialize DataIngestor with appropriate data file path
        data_ingestor = DataIngestor("../nutrition_activity_obesity_usa_subset.csv")
        # Define output file path
        output_file_path = f"output_best5.json"
        # Write the output to the output file
        with open(output_file_path, 'w') as f:
            output = data_ingestor.best5(question["question"])
            # convert the output to a serializable format
            output = convert_to_serializable(output)
            # write the output to the file
            json.dump(output, f)
        # Define the path to the expected result file
        result_file_path = f"../tests/best5/output/out-1.json"
        # Read the content of the output file
        with open(output_file_path, 'r') as f:
            output = f.read()
        # Read the content of the expected result file
        with open(result_file_path, 'r') as f:
            result = f.read()
        # Compare the output with the expected result
        self.assertEqual(output, result)

    def test_worst5_request(self):
        question = {"question": "Percent of adults aged 18 years and older who have an overweight classification"}
        # Initialize DataIngestor with appropriate data file path
        data_ingestor = DataIngestor("../nutrition_activity_obesity_usa_subset.csv")
        # Define output file path
        output_file_path = f"output_worst5.json"
        # Write the output to the output file
        with open(output_file_path, 'w') as f:
            output = data_ingestor.worst5(question["question"])
            # convert the output to a serializable format
            output = convert_to_serializable(output)
            # write the output to the file
            json.dump(output, f)
        # Define the path to the expected result file
        result_file_path = f"../tests/worst5/output/out-1.json"
        # Read the content of the output file
        with open(output_file_path, 'r') as f:
            output = f.read()
        # Read the content of the expected result file
        with open(result_file_path, 'r') as f:
            result = f.read()
        # Compare the output with the expected result
        self.assertEqual(output, result)

    def test_global_mean_request(self):
        question = {"question": "Percent of adults aged 18 years and older who have an overweight classification"}
        # Initialize DataIngestor with appropriate data file path
        data_ingestor = DataIngestor("../nutrition_activity_obesity_usa_subset.csv")
        # Define output file path
        output_file_path = f"output_global_mean.json"
        # Write the output to the output file
        with open(output_file_path, 'w') as f:
            output = data_ingestor.global_mean(question["question"])
            # convert the output to a serializable format
            output = convert_to_serializable(output)
            # write the output to the file
            json.dump(output, f)
        # Define the path to the expected result file
        result_file_path = f"../tests/global_mean/output/out-1.json"
        # Read the content of the output file
        with open(output_file_path, 'r') as f:
            output = f.read()
        # Read the content of the expected result file
        with open(result_file_path, 'r') as f:
            result = f.read()
        # Compare the output with the expected result
        self.assertEqual(output, result)

    def test_diff_from_mean_request(self):
        question = {"question": "Percent of adults aged 18 years and older who have an overweight classification"}
        # Initialize DataIngestor with appropriate data file path
        data_ingestor = DataIngestor("../nutrition_activity_obesity_usa_subset.csv")
        # Define output file path
        output_file_path = f"output_diff_from_mean.json"
        # Write the output to the output file
        with open(output_file_path, 'w') as f:
            output = data_ingestor.diff_from_mean(question["question"])
            json.dump(output, f)
        # Define the path to the expected result file
        result_file_path = f"../tests/diff_from_mean/output/out-1.json"
        # Read the content of the output file
        with open(output_file_path, 'r') as f:
            output = f.read()
        # Read the content of the expected result file
        with open(result_file_path, 'r') as f:
            result = f.read()

        # Compare the output with the expected result
        self.assertEqual(output, result)

    def test_state_diff_from_mean_request(self):
        question = {"question": "Percent of adults who report consuming vegetables less than one time daily", "state": "Virgin Islands"}
        # Initialize DataIngestor with appropriate data file path
        data_ingestor = DataIngestor("../nutrition_activity_obesity_usa_subset.csv")
        # Define output file path
        output_file_path = f"output_state_diff_from_mean.json"
        # Write the output to the output file
        with open(output_file_path, 'w') as f:
            output = data_ingestor.state_diff_from_mean(question["state"], question["question"])
            json.dump(output, f)
        # Define the path to the expected result file
        result_file_path = f"../tests/state_diff_from_mean/output/out-1.json"
        # Read the content of the output file
        with open(output_file_path, 'r') as f:
            output = f.read()
        # Read the content of the expected result file
        with open(result_file_path, 'r') as f:
            result = f.read()

        # Compare the output with the expected result
        self.assertEqual(output, result)

    def test_mean_by_category_request(self):
        question = {"question": "Percent of adults aged 18 years and older who have an overweight classification"}
        # Initialize DataIngestor with appropriate data file path
        data_ingestor = DataIngestor("../nutrition_activity_obesity_usa_subset.csv")
        # Define output file path
        output_file_path = f"output_mean_by_category.json"
        # Write the output to the output file
        with open(output_file_path, 'w') as f:
            output = data_ingestor.mean_by_category(question["question"])
            # convert the output to a serializable format
            output = output.to_dict()
            # write the output to the file
            json.dump(output, f)
        # Define the path to the expected result file
        result_file_path = f"../tests/mean_by_category/output/out-1.json"
        # Read the content of the output file
        with open(output_file_path, 'r') as f:
            output = f.read()
        # Read the content of the expected result file
        with open(result_file_path, 'r') as f:
            result = f.read()
        # Compare the output with the expected result
        self.assertEqual(output, result)

    def test_state_mean_by_category_request(self):
        question = {"question": "Percent of adults aged 18 years and older who have an overweight classification",
                    "state": "Oklahoma"}
        # Initialize DataIngestor with appropriate data file path
        data_ingestor = DataIngestor("../nutrition_activity_obesity_usa_subset.csv")
        # Define output file path
        output_file_path = f"output_state_mean_by_category.json"
        # Write the output to the output file
        with open(output_file_path, 'w') as f:
            output = data_ingestor.state_mean_by_category(question["state"], question["question"])
            # convert the output to a serializable format
            output = convert_to_serializable(output)
            # write the output to the file
            json.dump(output, f)
        # Define the path to the expected result file
        result_file_path = f"../tests/state_mean_by_category/output/out-1.json"
        # Read the content of the output file
        with open(output_file_path, 'r') as f:
            output = f.read()
        # Read the content of the expected result file
        with open(result_file_path, 'r') as f:
            result = f.read()
        # Compare the output with the expected result
        self.assertEqual(output, result)
