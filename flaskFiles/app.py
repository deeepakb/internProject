from flask import Flask, render_template, request, jsonify, session
from chat_function import chat_function
import os

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Set a secret key for session management

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/chat', methods=['POST'])
def chat():
    user_message = request.json['message']
    
    # Initialize conversation history if it doesn't exist
    if 'conversation' not in session:
        session['conversation'] = []
    
    # Add user message to conversation history
    session['conversation'].append({'role': 'user', 'content': user_message})
    
    # Get response from your chat function
    response = chat_function(user_message, session['conversation'])
    
    # Add AI response to conversation history
    session['conversation'].append({'role': 'assistant', 'content': response})
    
    # Save the session
    session.modified = True
    
    return jsonify({'message': response})

if __name__ == '__main__':
    app.run(debug=False)
