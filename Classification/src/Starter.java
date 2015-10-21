//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.FileReader;
//import java.util.List;
//
//import org.apache.hadoop.fs.Path;
//import org.apache.mahout.classifier.ClassifierResult;
//import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
//import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
//import org.apache.mahout.classifier.naivebayes.training. bayes.common.BayesParameters;
//import org.apache.mahout.classifier.bayes.datastore.InMemoryBayesDatastore;
//import org.apache.mahout.classifier.bayes.exceptions.InvalidDatastoreException;
//import org.apache.mahout.classifier.bayes.interfaces.Algorithm;
//import org.apache.mahout.classifier.bayes.interfaces.Datastore;
//import org.apache.mahout.classifier.bayes.model.ClassifierContext;
//import org.apache.mahout.common.nlp.NGrams;
//
//public class Starter {
// public static void main( final String[] args ) {
//  final BayesParameters params = new BayesParameters();
//  params.setGramSize( 1 );
//  params.set( "verbose", "true" );
//  params.set( "classifierType", "bayes" );
//  params.set( "defaultCat", "OTHER" );
//  params.set( "encoding", "UTF-8" );
//  params.set( "alpha_i", "1.0" );
//  params.set( "dataSource", "hdfs" );
//  params.set( "basePath", "output" );
//  
//  try {
//      Path input = new Path("/tmp/mahout-work-mohammad/classification/input" );
//      Path outnput = new Path("/tmp/mahout-work-mohammad/classification/output");
//      //"/home/mohammad/workspace/Classification/outnput" );
//      TrainClassifier.trainCNaiveBayes(input, outnput, params );
//   
//      Algorithm algorithm = new BayesAlgorithm();
//      Datastore datastore = new InMemoryBayesDatastore( params );
//      ClassifierContext classifier = new ClassifierContext( algorithm, datastore );
//      classifier.initialize();
//      
//      final BufferedReader reader = new BufferedReader( new FileReader( args[ 0 ] ) );
//      String entry = reader.readLine();
//      
//      while( entry != null ) {
//          List< String > document = new NGrams( entry, 
//                          Integer.parseInt( params.get( "gramSize" ) ) )
//                          .generateNGramsWithoutLabel();
//
//          ClassifierResult result = classifier.classifyDocument( 
//                           document.toArray( new String[ document.size() ] ), 
//                           params.get( "defaultCat" ) );          
//
//          entry = reader.readLine();
//      }
//  } catch( final IOException ex ) {
//   ex.printStackTrace();
//  } catch( final InvalidDatastoreException ex ) {
//   ex.printStackTrace();
//  }
// }
//}
//
