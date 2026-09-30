import asyncio
import json
import logging
import os
from flask import Flask, request, jsonify
import websockets

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# DERIV API CREDENTIALS
DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")
DERIV_TOKEN = os.environ.get("DERIV_TOKEN", "")

async def execute_deriv_trade(symbol, contract_type, stake, duration, duration_unit):
    uri = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    async with websockets.connect(uri) as websocket:
        # 1. Authorize Account
        await websocket.send(json.dumps({"authorize": DERIV_TOKEN}))
        auth_response = json.loads(await websocket.recv())
        
        if "error" in auth_response:
            logging.error(f"Auth Error: {auth_response['error']['message']}")
            return False, auth_response['error']['message']

        # 2. Request Proposal
        proposal_request = {
            "proposal": 1,
            "amount": stake,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "duration": duration,
            "duration_unit": duration_unit,
            "symbol": symbol
        }
        await websocket.send(json.dumps(proposal_request))
        proposal_response = json.loads(await websocket.recv())

        if "error" in proposal_response:
            logging.error(f"Proposal Error: {proposal_response['error']['message']}")
            return False, proposal_response['error']['message']

        proposal_id = proposal_response["proposal"]["id"]

        # 3. Buy Contract
        buy_request = {"buy": proposal_id, "price": stake}
        await websocket.send(json.dumps(buy_request))
        buy_response = json.loads(await websocket.recv())

        if "error" in buy_response:
            logging.error(f"Purchase Error: {buy_response['error']['message']}")
            return False, buy_response['error']['message']

        return True, buy_response['buy']['contract_id']

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    logging.info(f"Signal Received: {data}")

    symbol = data.get("symbol", "R_50")
    contract_type = data.get("contract_type", "CALL")
    stake = float(data.get("stake", 10))
    duration = int(data.get("duration", 5))
    duration_unit = data.get("duration_unit", "m")

    success, result = asyncio.run(execute_deriv_trade(symbol, contract_type, stake, duration, duration_unit))
    
    if success:
        return jsonify({"status": "SUCCESS", "contract_id": result}), 200
    else:
        return jsonify({"status": "FAILED", "error": result}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
