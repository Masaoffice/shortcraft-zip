import express from "express";
import archiver from "archiver";

const app = express();
const PORT = process.env.PORT || 8080;

app.get("/", (req, res) => {
  res.status(200).send("shortcraft-zip OK");
});

app.post("/api/create-zip", (req, res) => {
  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", 'attachment; filename="test.zip"');

  const archive = archiver("zip");
  archive.pipe(res);

  archive.append("hello shortcraft", { name: "hello.txt" });
  archive.finalize();
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server listening on port ${PORT}`);
});
