from flask import Flask, render_template, request, redirect, url_for
import boto3

app = Flask(__name__)

# Configure AWS
client = boto3.client('textract', region_name='us-east-1')
translate_client = boto3.client('translate', region_name='us-east-1')

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/textract', methods=['GET', 'POST'])
def textract_page():
    if request.method == 'POST':
        file = request.files['file']
        if file:
            # Process file with AWS Textract
            response = client.analyze_document(
                Document={'Bytes': file.read()},
                FeatureTypes=['TABLES', 'FORMS'])
            extracted_text = ' '.join([item['Text'] for item in response['Blocks'] if item['BlockType'] == 'LINE'])
            return render_template('textract.html', text=extracted_text)
        return redirect(url_for('error', msg='No file uploaded'))
    return render_template('textract.html', text=None)

@app.route('/translate', methods=['GET', 'POST'])
def translate_page():
    if request.method == 'POST':
        file = request.files['file']
        target_language = request.form['language']
        if file:
            text = file.read().decode('utf-8')
            # Translate text using AWS Translate
            result = translate_client.translate_text(
                Text=text,
                SourceLanguageCode='auto',
                TargetLanguageCode=target_language)
            translated_text = result.get('TranslatedText')
            return render_template('translate.html', translation=translated_text)
        return redirect(url_for('error', msg='No file uploaded or language selected'))
    return render_template('translate.html', translation=None)

@app.route('/error')
def error():
    msg = request.args.get('msg', 'An error occurred')
    return render_template('error.html', message=msg)

if __name__ == '__main__':
    app.run(debug=True)

