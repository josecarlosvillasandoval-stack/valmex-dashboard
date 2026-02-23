from flask import Flask, send_file, request, session, redirect, jsonify
import os

app = Flask(__name__)
app.secret_key = "valmex_secret_2024"

BASE = os.path.dirname(os.path.abspath(__file__))

USERS = {
    "jvilla":    {"nombre": "José Carlos Villa", "password": "valmex", "rol": "admin", "iniciales": "JV"},
    "emartino":  {"nombre": "Emilio Martino",    "password": "valmex", "rol": "vista", "iniciales": "EM"},
    "oscargar":  {"nombre": "Oscar García",       "password": "valmex", "rol": "vista", "iniciales": "OG"}
}

@app.route("/")
def index():
    if "usuario" not in session:
        return send_file(os.path.join(BASE, "login.html"))
    return send_file(os.path.join(BASE, "valmex_dashboard.html"))

@app.route("/login", methods=["POST"])
def login():
    data     = request.get_json()
    usuario  = data.get("usuario", "").strip()
    password = data.get("password", "").strip()
    user     = USERS.get(usuario)
    if user and user["password"] == password:
        session["usuario"]   = usuario
        session["nombre"]    = user["nombre"]
        session["rol"]       = user["rol"]
        session["iniciales"] = user["iniciales"]
        return jsonify({"ok": True, "nombre": user["nombre"], "rol": user["rol"], "iniciales": user["iniciales"]})
    return jsonify({"ok": False}), 401

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

@app.route("/me")
def me():
    if "usuario" not in session:
        return jsonify({"ok": False}), 401
    return jsonify({"ok": True, "nombre": session["nombre"], "rol": session["rol"], "iniciales": session["iniciales"]})

@app.route("/PC.pdf")
def pdf():
    if "usuario" not in session:
        return redirect("/")
    return send_file(os.path.join(BASE, "PC.pdf"))

@app.route("/VALMEX.png")
def logo1():
    return send_file(os.path.join(BASE, "VALMEX.png"))

@app.route("/VALMEX2.png")
def logo2():
    return send_file(os.path.join(BASE, "VALMEX2.png"))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
