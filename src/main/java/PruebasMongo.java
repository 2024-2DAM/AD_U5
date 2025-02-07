import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class PruebasMongo {
    public static void main(String[] args) {
        //Conexión a la BD:
        String url = "mongodb://root:Sandia4you@localhost:27017";
        MongoCollection<Document> cShooters;
        try (MongoClient cliente = MongoClients.create(url);) {
            System.out.println("Conectado :)");

            Iterable<String> db = cliente.listDatabaseNames();
            for (String dbName : db) {
                System.out.println(dbName);
                //Obtengo una base de datos e itero en sus colecciones:
                MongoDatabase mongoDatabase = cliente.getDatabase(dbName);
                Iterable<String> collections = mongoDatabase.listCollectionNames();
                for (String collectionName : collections) {
                    System.out.println("\t" + collectionName);
                }
            }

            //Me conecto a la base de datos games:
            MongoDatabase dbGames = cliente.getDatabase("games");
            //Creo la colección shooters (si ya existe, no la borra)
            dbGames.createCollection("shooters");
            cShooters = dbGames.getCollection("shooters");

            //Inserto un Document (JSON):
            Document cod = new Document();
            //String con título: clave: title - valor: Call of Duty II
            cod.append("title", "Minecraft");
            //int año de lanzamiento:
            cod.append("release_year", 2005);
            cod.append("price", 12.6);

            //Array con las consolas compatibles:
            List<String> devices = new ArrayList<>();
            devices.add("PS2");
            devices.add("XBox");
            devices.add("PC");
            cod.append("devices", devices);

            //Documento embebido: Requirements
            Document req = new Document();
            req.append("storage", 9);
            req.append("ram", 100);
            req.append("cpu", 1.9);
            cod.append("req", req);

            //Comento esta línea para que no se introduzca un nuevo doc cada vez que lo ejecuto
            //cShooters.insertOne(cod);

            //Read:
            //leo todos los juegos:
            Iterable<Document> games = cShooters.find();
            for (Document game : games) {
                System.out.println("Título: " + game.getString("title"));
                System.out.println("Price: " + game.getDouble("price"));
                //Así con el resto de pares clave-valor con valor sencillo
                //Lista:
                List<String> dev = game.getList("devices", String.class);
                System.out.println("Devices: " + dev);


                //Leer el documento emebebido "req":
                Document r = game.get("req", Document.class);
                System.out.println("Almacenamiento: " + r.getInteger("storage"));
                System.out.println("RAM: " + r.getInteger("ram"));
                System.out.println("CPU: " + r.getDouble("cpu"));

                //Valor Array:
                System.out.println("**************************");
            }

            System.out.println("---------------------------");
            buscarPorNombre(cShooters, "Min");
        }
    }

    //
    public static void buscarPorNombre(MongoCollection<Document> collection, String nombre) {
        //Quiero hacer en mongo esto: db.shooters.find({title:"Call of Duty II"})
        //La variable filtro es: {title:"Call of Duty II"}
        Bson filtro = Filters.eq("title", nombre);
        Iterable<Document> games = collection.find(filtro);
        for (Document game : games) {
            System.out.println("Titulo: " + game.getString("title"));
        }
    }

}
