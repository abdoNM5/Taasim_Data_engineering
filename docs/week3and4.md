1. The Week 6 Notebook (week6.ipynb)
The Goal: Train an AI to predict future taxi demand.

What it did: It acted as your Data Science lab. It connected to MinIO (where your historical taxi data is stored), cleaned up the data, and created "features" (like what day of the week it is, what hour it is, and what demand was like 7 days ago).
The Result: It trained a Machine Learning model called a GBT (Gradient Boosted Tree) to learn the patterns of taxi demand. Once the model was smart enough, the notebook took that "brain" (the model) and saved it as a permanent file back into MinIO so other apps could use it.
2. The API Stub (api_stub.py)
The Goal: Serve the AI's brain to the outside world.

What it did: The notebook is great for training, but it's not a live application. Your api_stub.py is a web server (using FastAPI).
The Result: When you started the API, it reached into MinIO, downloaded the "brain" you trained in the notebook, and kept it actively running in its memory. It set up a dedicated URL (/api/demand/forecast) that constantly listens for incoming questions.
3. The Command You Just Ran
The Goal: Ask the live AI a question.

What it did: Invoke-RestMethod is just a command-line way of sending a message over the internet (like a browser does, but behind the scenes).
The Result: You sent a message to your live API saying: "Hey, what will the taxi demand be in Zone 1 on July 1st, 2023 at 8:00 AM?"
The API received your message, ran it through the Machine Learning "brain", calculated the answer, and sent the answer back to your screen (5.699 cars).
In short: The notebook built and trained the AI. The API hosts the AI so it can talk to the real world. The command was you having a conversation with it!