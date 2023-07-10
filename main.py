import PyPDF2
import nltk
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import psycopg2
from psycopg2 import Error

nltk.download('punkt')
nltk.download('stopwords')

def extract_keywords_from_pdf(file_path):
    # Open the PDF file
    with open(file_path, 'rb') as file:
        pdf = PyPDF2.PdfReader(file)
        text = ''
        # Extract text from each page
        for page in pdf.pages:
            text += page.extract_text()

    # Tokenize the text into words
    tokens = word_tokenize(text)

    # Remove stopwords
    stop_words = set(stopwords.words('english'))
    keywords = [word.lower() for word in tokens if word.lower() not in stop_words]

    return keywords

def remove_punctuations(keywords):
    result = []
    for item in keywords:
        item_without_punctuations = "".join(char for char in item if char not in string.punctuation)
        result.append(item_without_punctuations)
    return result

pdf_file_path = "C:/Users/viswa/Downloads/sample-pdf-file.pdf"
keywords = extract_keywords_from_pdf(pdf_file_path)
print(keywords)

clean_list = remove_punctuations(keywords)
final_list = list(set(clean_list))
print(final_list[1:])

# Establish connection to PostgreSQL
try:
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",        port="5432",
        user="postgres",
        password="Viswa@2003"
    )
    cursor = connection.cursor()
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL:", error)

# Create table if it doesn't exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS keywords (
    id SERIAL PRIMARY KEY,
    keyword TEXT
);
'''
try:
    cursor.execute(create_table_query)
    connection.commit()
    print("Table created successfully.")
except (Exception, Error) as error:
    print("Error while creating table in PostgreSQL:", error)

# Insert or update data into the table
insert_query = "INSERT INTO keywords (keyword) VALUES (%s);"
update_query = "UPDATE keywords SET keyword = %s WHERE keyword = %s;"
try:
    for keyword in final_list:
        cursor.execute(insert_query, (keyword,))
        if cursor.rowcount == 0:
            cursor.execute(update_query, (keyword, keyword))
    connection.commit()
    print("Data inserted/updated successfully.")
except (Exception, Error) as error:
    print("Error while inserting/updating data into PostgreSQL:", error)

# Close cursor and connection
cursor.close()
connection.close()
