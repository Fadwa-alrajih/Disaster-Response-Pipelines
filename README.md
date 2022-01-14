# Disaster Response Pipeline Project
## Description:
In this app, a machine learning pipeline is created to classify the emergency messages sent by users to appropriate categories.
The data for training is obtained from Figure Eight [Disaster Response Data](https://www.figure-eight.com/dataset/combined-disaster-response-data/). The data thus obtained is cleaned and transformed to the necessary form using the ETL pipelines and other Data Engineering techniques. Then a front-end is created using the HTML and deployed using the python FLASK library. Visit the below link for more information on [Flask](https://flask.palletsprojects.com/en/1.1.x/).
## File Structure of the application:
The file structure is as follows:
1. There are 3 main folders
    a) data: This folder has the data files in the form of csv as well as the process_data.py file. Also the database named "DisasterResponse.db" is present.
    
    b) model: This is the folder that contains the machine learning classifier file and the python file to train the model. It is the train_classifier.py
    
    c) app: Here the front-end/web-app related html and python files are present
    
    d) Jupyter Notebooks: Notebooks are present in thi directory. These are useful for the purpose of visualization.
    
    Files:
```
- app
| - template
| |- master.html  # main page of web app
| |- go.html  # classification result page of web app
|- run.py  # Flask file that runs app
- data
|- disaster_categories.csv  # data to process
|- disaster_messages.csv  # data to process
|- process_data.py
|- DisasterResponse.db   # database to save clean data
- models
|- train_classifier.py
|- classifier.pkl  # saved model
- Jupyter Notebooks
|- ETL Pipeline Preparation.ipynb # etl exploration
|- ML Pipeline Preparation.ipynb # ML exploration
- README.md
```
 
## Required packages:
* flask
* joblib
* jupyter If you want to view the notebooks
* pandas
* plot.ly
* numpy
* scikit-learn
* sqlalchemy
### Instructions to run the App:
1. Clone the repository completely.
2. Run the following commands in the project's root directory to set up your database and model.
    - To run ETL pipeline that cleans data and stores in database
        `python data/process_data.py data/disaster_messages.csv data/disaster_categories.csv data/DisasterResponse.db`
    - To run ML pipeline that trains classifier and saves
        `python models/train_classifier.py data/DisasterResponse.db models/classifier.pkl`
3. Run the following command in the app's directory to run your web app.
    `python run.py`
4. Go to http://0.0.0.0:3001/ and if you are running in the local machine go to http://localhost:3001/.
## Enhancements:
This application can be improved in the following areas:
* This application performs a below the expectations in the implied statements. 
    For Example: If you type "Low on Supplies" in the text-box then the model fails to categorize to the correct category.
    #### To improve this the Natural Language Processing techniques have to applied, which is out of scope here.
    
*  It relies a bit more on the keywords. To improve this NLTK python package can be used.
*  From the second graph below it clear that, some of the categories do not get the same weigtage in training, due
   to less number of training data for the same. This leads to lower accuracy in the classification for these classes. This can
   be obtained by obtaining more training and test data for the same from various sources.

## Explanation of the screenshots:
## Explanation of the app:

In this section the app is explained:
1. The below is the screenshot for the first page where the user input is taken. As shown below the user input is "My friend is stranded at his house with low supplies of food". The classifier classifies this as the "AID related" and the label taken is food.
1. The first page where the user input is taken.The classifier classifies it.

2. The first page also contains the training set graphs plotted using the plotly. These training data is provided by the Figure Eight. The images is as shown below. 
2. The first page also contains the training set graphs plotted using the plotly. These training data is provided by the Figure Eight.

* Here the First graph shows the classification of the Data sources, from social media, news, and direct. The count of the number of the messages obtained from each source is plotted.
# Disaster Response Pipeline Project
## Description:
In this app, a machine learning pipeline is created to classify the emergency messages sent by users to appropriate categories.
The data for training is obtained from Figure Eight [Disaster Response Data](https://www.figure-eight.com/dataset/combined-disaster-response-data/). The data thus obtained is cleaned and transformed to the necessary form using the ETL pipelines and other Data Engineering techniques. Then a front-end is created using the HTML and deployed using the python FLASK library. Visit the below link for more information on [Flask](https://flask.palletsprojects.com/en/1.1.x/).
## File Structure of the application:
The file structure is as follows:
1. There are 3 main folders
    a) data: This folder has the data files in the form of csv as well as the process_data.py file. Also the database named "DisasterResponse.db" is present.
    
    b) model: This is the folder that contains the machine learning classifier file and the python file to train the model. It is the train_classifier.py
    
    c) app: Here the front-end/web-app related html and python files are present
    
    d) Jupyter Notebooks: Notebooks are present in thi directory. These are useful for the purpose of visualization.
    
    Files:
```
- app
| - template
| |- master.html  # main page of web app
| |- go.html  # classification result page of web app
|- run.py  # Flask file that runs app
- data
|- disaster_categories.csv  # data to process
|- disaster_messages.csv  # data to process
|- process_data.py
|- DisasterResponse.db   # database to save clean data
- models
|- train_classifier.py
|- classifier.pkl  # saved model
- Jupyter Notebooks
|- ETL Pipeline Preparation.ipynb # etl exploration
|- ML Pipeline Preparation.ipynb # ML exploration
- README.md
```
 
## Required packages:
* flask
* joblib
* jupyter If you want to view the notebooks
* pandas
* plot.ly
* numpy
* scikit-learn
* sqlalchemy
### Instructions to run the App:
1. Clone the repository completely.
2. Run the following commands in the project's root directory to set up your database and model.
    - To run ETL pipeline that cleans data and stores in database
        `python data/process_data.py data/disaster_messages.csv data/disaster_categories.csv data/DisasterResponse.db`
    - To run ML pipeline that trains classifier and saves
        `python models/train_classifier.py data/DisasterResponse.db models/classifier.pkl`
3. Run the following command in the app's directory to run your web app.
    `python run.py`
4. Go to http://0.0.0.0:3001/ and if you are running in the local machine go to http://localhost:3001/.
## Enhancements:
This application can be improved in the following areas:
* This application performs a below the expectations in the implied statements. 
    For Example: If you type "Low on Supplies" in the text-box then the model fails to categorize to the correct category.
    #### To improve this the Natural Language Processing techniques have to applied, which is out of scope here.
    
*  It relies a bit more on the keywords. To improve this NLTK python package can be used.
*  From the second graph below it clear that, some of the categories do not get the same weigtage in training, due
   to less number of training data for the same. This leads to lower accuracy in the classification for these classes. This can
   be obtained by obtaining more training and test data for the same from various sources.

## Explanation of the screenshots:
## Explanation of the app:

In this section the app is explained:
1. The below is the screenshot for the first page where the user input is taken. As shown below the user input is "My friend is stranded at his house with low supplies of food". The classifier classifies this as the "AID related" and the label taken is food.
1. The first page where the user input is taken.The classifier classifies it.

2. The first page also contains the training set graphs plotted using the plotly. These training data is provided by the Figure Eight. The images is as shown below. 


* Here the First graph shows the classification of the Data sources, from social media, news, and direct. The count of the number of the messages obtained from each source is plotted.
![image](https://user-images.githubusercontent.com/94748110/149564614-e8ad475b-9237-459e-8c39-f897de31fdbf.png)

* The second graph displays the  number of message in each of the the disaster response categories.
* ![image](https://user-images.githubusercontent.com/94748110/149564386-4e45630f-d4b0-47e7-90e7-312e67dd5777.png)
