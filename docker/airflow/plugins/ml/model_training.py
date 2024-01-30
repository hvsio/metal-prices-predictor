from model import Model

def train():
    model = Model(["XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD"], 12, 1)
    model.train(use_generated_data=True)
    model.save("/tmp/model1")

