from flask import Flask, render_template
from flask import request
from search_logic import getMergedResults

app = Flask(__name__)
app.debug = True
app.secret_key = 'development key'


@app.route("/")
def base():
	return render_template("searchHome.html")

@app.route("/search", methods=['GET'])
def search():
	if len(request.args) > 0 and "searchText" in request.args:
		searchText = request.args.get("searchText")
		terms = searchText.strip().split(' ')
		docs = getMergedResults(terms)
		docs['searchText'] = searchText
		return render_template("searchHome.html",data=docs)
	else:
		return render_template("searchHome.html",data={searchText:""})

if __name__ == "__main__":
	app.run()