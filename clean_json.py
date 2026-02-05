import json
import pandas as pd

techs: list[str] = ["airflow"]

for tech in techs:

    with open(f"input_from_aws/{tech}/questions.json") as questions_file:
        questions_df = pd.DataFrame.from_dict(json.load(questions_file)["items"])
        questions_df = questions_df[["question_id", "title", "body"]]
        print(questions_df.columns)

    with open(f"input_from_aws/{tech}/answers.json") as answers_file:
        answers_df = pd.DataFrame.from_dict(json.load(answers_file)["items"])
        answers_df = answers_df[["question_id", "body"]]

    combined_df = pd.merge(questions_df, answers_df, how="right", left_on="question_id", right_on="question_id", suffixes=("_question", "_answer"))
    print(combined_df.columns)
    
    
    for index, row in combined_df.iterrows():
        temp_dict = {"title": row["title"], "question" : row["body_question"], "answer" : row["body_answer"]}
        
        question_id = row["question_id"]
        file_name = f"cleaned_aws/{question_id}.json"
        with open(file_name, 'w') as temp_file:
            json.dump(temp_dict, temp_file)
    
