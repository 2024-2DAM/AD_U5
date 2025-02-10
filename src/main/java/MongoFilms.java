import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;


public class MongoFilms {
    public static void main(String[] args) {
        System.out.println(getFilms2010());
        getFilmsBetweenRating(7.1,9.6);
        updateLanguagesByTitle("The Pillars of the Earth");
    }

    public static MongoDatabase getCollection() {
        //Conexión a la BD: MongoDatabase
        String url = "mongodb://root:Sandia4you@localhost:27017";
        MongoClient cliente = MongoClients.create(url);
        return cliente.getDatabase("filmLibrary");
    }

    public static List<String> getFilms2010() {
        //db.films.find({year:2010}, {title:true, _id:false})

        //Cojo la colección:
        MongoDatabase database = getCollection();
        MongoCollection<Document> collection = database.getCollection("films");

        //Pongo los criterios de búsqueda (Filter)
        //Bson filters = Filters.gt("imdb.rating", 7.9);    //Pelis con puntuación (imdb.rating) > 7.9
        Bson filters = Filters.eq("year", 2010);

        //Pongo los criterios de proyección (Projection)
        Bson titleProjection = Projections.include("title");
        Bson idProjection = Projections.excludeId();
        Bson projection = Projections.fields(titleProjection, idProjection);

        //Lo de arriba es equivalente a esto:
//        Bson titleProjection = Filters.eq("title", true);
//        Bson idProjection = Filters.eq("_id", false);
//        Bson projection = Filters.and(titleProjection, idProjection);

        //Hago la búsqueda
        Iterable<Document> busqueda = collection.find(filters).projection(projection);

        //Preparo el retorno
        List<String> films = new ArrayList<>();
        for (Document b : busqueda) {
            films.add(b.getString("title"));
        }
        return films;
    }

    public static List<Film> getFilmsBetweenRating(double min, double max) {
        //Collection:
        MongoDatabase database = getCollection();
        MongoCollection<Document> collection = database.getCollection("films");

        //Filtro de búsqueda:
        Bson minFilter = Filters.gte("imdb.rating", min);
        Bson maxFilter = Filters.lte("imdb.rating", max);
        Bson filter = Filters.and(minFilter, maxFilter);

        //Hago la búsqueda:
        Iterable<Document> result = collection.find(filter);

        //Preparo el retorno:
        List<Film> pelis = new ArrayList<>();
        for(Document r: result){
            String title = r.getString("title");
            int year = r.getInteger("year");
            List<String> genres = r.getList("genres", String.class);
            pelis.add(new Film(title, year, genres));
        }

        return pelis;

    }


    /**
     * Añado el lenguaje "Java" a la película que estoy buscando por título, y le cambio
     * el rating a "NO".
     * @param title
     */
    public static void updateLanguagesByTitle(String title) {
        //db.films.updateOne({title: XXXX},{and:{$push:{languages:"Java"}},$set:{rated:"NO}})
        MongoDatabase database = getCollection();
        MongoCollection<Document> collection = database.getCollection("films");
        //Filtro de búsqueda
        Bson filters = Filters.eq("title", title);
        //Documente de actualización
        Bson updateLanguage = Updates.push("languages", "Java");
        Bson updateRated = Updates.set("rated", "NO");
        Bson updates = Updates.combine(updateLanguage, updateRated);

        collection.updateOne(filters, updates);
    }






}
