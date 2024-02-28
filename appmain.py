import logging
import boto3
import json
from decimal import Decimal
from flask import Flask, render_template, request, redirect
from models import RPGItems, Vehicles
from ExcelDBMigration import markupToImport
app = Flask(__name__)

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

@app.route('/')
def index():
	app.logger.debug('this is a DEBUG message')
	app.logger.info('this is an INFO message')
	app.logger.warning('this is a WARNING message')
	app.logger.error('this is an ERROR message')
	app.logger.critical('this is a CRITICAL message')
	itemList = RPGItems.scan()
	vehicleList = Vehicles.scan()
	return render_template('start.html', 
		items = itemList, 
		vehicles = vehicleList,
		schema = RPGItems.getSchema(),
		vehicleSchema = Vehicles.getSchema())

@app.route('/migration', methods = ['GET','POST'])
def migration():
	if request.files:
		try:
			markupToImport(request.files["markedSheet"])
		except Exception as error:
			return str(error)
		return redirect("/")
	return render_template("migrationForm.html")

@app.route("/items/new",methods=['GET', 'POST'])
def create():
	"""Add new item"""
	if request.form:
		try:
			#construct initial dictionary
			initDict = {}
			for column in RPGItems.getSchema():
				if column in request.form.keys():
					initDict[column] = request.form[column]
				if column in request.files.keys():
					initDict[column] = request.files[column]
			RPGItems(initDict)
		except Exception as error:
			return str(error)
		return redirect("/")
	return render_template("createItem.html",
        		title = "Add New Item",
			schema = RPGItems.getSchema())

@app.route('/items/<name>')
def viewItem(name):
	item = RPGItems.load(name)
	return render_template('viewItem.html', item = item)

@app.route("/items/<name>/edit",methods=['GET', 'POST'])
def edit(name):
	"""Edit given item"""
	#load item first
	item = RPGItems.load(name)
	if request.form:
		try:
			#file = request.files["picture"]
			#app.logger.info(file.__class__)
			#s3Client = boto3.client('s3')
			#response = s3Client.upload_fileobj(file,"gdbb", file.filename)
			#raise Exception("Successfully uploaded file type is: "+repr(file.__class__))
			for column in RPGItems.getSchema():
				if column in request.form.keys():
					setattr(item,column, request.form[column])
				if column in request.files.keys():
					setattr(item,column, request.files[column])
			item.save()
		except Exception as error:
			return str(error)
		return redirect("/")
	return render_template("editItem.html", item = item)

@app.route("/items/<name>/delete")
def delete(name):
	item = RPGItems.load(name)
	item.delete()
	return redirect("/")



@app.route("/vehicles/new")
def createVehicle():
	"""Add new vehicle"""
	if request.args:
		try:
			newVehicle = {}
			for column in Vehicles.getSchema():
				newVehicle[column] = ""
				if column in request.args.keys():
					newVehicle[column] = request.args[column]
			Vehicles(newVehicle)
		except Exception as error:
			return str(error)
		return redirect("/")
	return render_template("createItem.html",
        		title = "Add New Vehicle",
			schema = Vehicles.getSchema())

@app.route('/vehicles/<id>')
def viewVehicle(id):
	vehicle = Vehicles.load(id)
	return render_template('viewItem.html', item = vehicle)

@app.route("/vehicles/<id>/edit",methods=['GET', 'POST'])
def editVehicle(id):
	"""Edit given vehicle"""
	#load vehicle first
	vehicle = Vehicles.load(id)
	if request.form:
		try:
			for column in Vehicles.getSchema():
				if column in request.form.keys():
					setattr(vehicle,column, request.form[column])
				if column in request.files.keys():
					setattr(vehicle,column, request.files[column])
			vehicle.save()
		except Exception as error:
			return str(error)
		return redirect("/")
	return render_template("editItem.html", item = vehicle)

@app.route("/vehicles/<id>/delete")
def deleteVehicle(id):
	vehicle = Vehicles.load(id)
	vehicle.delete()
	return redirect("/")


@app.route('/path/<p1>', methods=['GET'])
def path(p1):
	#return "Parameter 1 = " + parameter1 + ", Parameter 2 = " + parameter2
	request_params = request.args
	return render_template('index.html', username=str(p1), params=request.args.get("name"))

@app.route('/auth')
def auth():
	return "Authentication successful"


@app.route('/chrisStuff')
def chrisStuff():
	return "Hi from Chris"

@app.errorhandler(404)
def page_not_found(e):
	return render_template("error.html"), 404



if __name__ == "__main__":
	app.run(host="0.0.0.0", port=5000, debug=True)
