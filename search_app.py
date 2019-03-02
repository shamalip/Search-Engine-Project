from flask import Flask, render_template
from flask import request
from search_logic import get_search_results

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
		docs = get_search_results(searchText,terms)
		docs['searchText'] = searchText
		print('rendering')
		return render_template("searchHome.html",data=docs)
	else:
		return render_template("searchHome.html",data={searchText:""})

if __name__ == "__main__":
	app.run()