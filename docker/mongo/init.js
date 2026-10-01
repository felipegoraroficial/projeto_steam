db = db.getSiblingDB("steam");

db.createCollection("playtime");

db.playtime.createIndex(
  {
    captured_at: 1
  },
  {
    expireAfterSeconds: 2592000
  }
);

db.playtime.insertOne({
  initialized: true,
  createdAt: new Date()
});