// # Iterating docs and positioms.
//
// At its core of tantivy, relies on a data structure
// called an inverted index.
//
// This example shows how to manually iterate through
// the list of documents containing a term, getting
// its term frequency, and accessing its positions.

// ---
// Importing tantivy...
#[macro_use]
extern crate tantivy;
extern crate rulinalg;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::{DocId, DocSet, Postings};

use std::collections::HashMap;

use rulinalg::matrix::{BaseMatrixMut, BaseMatrix, Matrix};
use rulinalg::matrix::decomposition::FullPivLu;
use std::str;

#[macro_use] extern crate prettytable;
use prettytable::{Table, Row, Cell};

fn simplify(a: f32) -> f32{
    if (a > 0.2) {
        1.0
    } else {
        0.0
    }
}

fn main() -> tantivy::Result<()> {
    // Get result rows from tantivy.
    let (index, field) = create_test_index()?;
    let tantivy_result = get_tantivy_matrix(index, field)?;
    let ak = calculate_lsa(tantivy_result);
    Ok(())
}

fn calculate_lsa(tantivy_result: Vec<TantivyDocTermFreq>) -> tantivy::Result<Matrix<f32>> {

    // Calculate number of documents.
    let mut max_docid = 0;
    for record in &tantivy_result {
        if record.doc_id > max_docid {
            max_docid = record.doc_id;
        }
    }

    // Map terms (strings) to ids (ints).
    let mut terms_map = HashMap::new();
    let mut num_terms = 0;
    for record in &tantivy_result {
        if let Some(_) = terms_map.get(&record.term) {
        } else {
            terms_map.insert(record.term.clone(), num_terms);
            num_terms = num_terms + 1;
        }
    }

    println!("terms map {:?}", terms_map);
    println!("max_docid {:?}", max_docid);
    println!("num_terms {:?}", num_terms);

    // Create zeroed matrix with docs as rows and terms as columns.
    let mut a = Matrix::<f32>::zeros(num_terms, max_docid as usize + 1);

    // Iterate over result rows.
    for record in &tantivy_result {
        let term_index = terms_map.get(&record.term).unwrap();
        println!("record {:?} {:?} {:?}", record.doc_id, *term_index, record.term_freq);

        // Set value in matrix.
        let mut slice = a.sub_slice_mut([*term_index, record.doc_id as usize], 1, 1);
        let value = slice.iter_mut().next().unwrap();
        *value = record.term_freq as f32;
    }

    print_term_table(&terms_map, &a, "a");

    let svd = a.clone().svd().unwrap();
    let (s, mut u, v) = svd;
    // let (s, v, mut u) = svd;
    print_matrix_table(s, "s");
    print_matrix_table(v, "v");
    print_matrix_table(u.clone(), "u");

     let b = a.transpose();
    let c = &a * &b;
    //let c = a.clone();
    let lu = FullPivLu::decompose(c).unwrap();
    let rank = lu.rank();
    println!("rank {:#?}", rank);

    // let k = rank - 1;
    let k = 3;

    let uk = u.sub_slice_mut([0, 0], u.rows(), k);
    let uk_transposed = u.sub_slice_mut([0, 0], u.rows(), k).transpose();
    let t = &uk * &uk_transposed;

    print_matrix_table(t.clone(), "t");

    let ak = &t * a.clone();

    // calculate synonyms?
    let threshold = 0.5;
    let ak_simplified = ak.clone().apply(&simplify);

    print_term_table(&terms_map, &a, "a");
    print_term_table(&terms_map, &ak, "ak");
    print_term_table(&terms_map, &ak_simplified, "ak_simplified");

    // let x = &a - &ak_simplified;
    // print_term_table(&terms_map, &x, "x");

    Ok(ak)
}

fn print_matrix_table (matrix: Matrix<f32>, message: &str) {
    let mut table = Table::new();
    for row in matrix.row_iter() {
        let mut fields = vec!();
        for val in row.iter() {
            let formatted_val = (val * 100.0).round() / 100.0;
            fields.push(Cell::new(&formatted_val.to_string()));
        }
        table.add_row(Row::new(fields));
    }
    println!("\n{}\n", message);
    table.printstd();
}

fn print_term_table (terms_map: &HashMap<String, usize>, matrix: &Matrix<f32>, message: &str) {
    let mut table = Table::new();
    // let mut header = vec!(Cell::new("id"));
    let mut terms = vec!();
    for (key, _id) in terms_map {
        terms.push(key);
    }
    terms.sort();
    // table.add_row(Row::new(header));
    let mut i = 0;

    for row in matrix.row_iter() {
        let mut fields = vec!();
        // fields.push(Cell::new(&i.to_string()));
        // let term = terms_map[i];
        if i < terms.len() {
            fields.push(Cell::new(terms[i]));
        } else {
            fields.push(Cell::new(""));
        }
        for val in row.iter() {
            let formatted_val = (val * 10.0).round();
            fields.push(Cell::new(&formatted_val.to_string()));
        }
        table.add_row(Row::new(fields));
        i = i + 1;
    }
    // Add a row per time
    println!("\n{}\n", message);
    table.printstd();
}

#[derive(Debug)]
struct TantivyDocTermFreq {
    doc_id: DocId,
    term_freq: u32,
    // text: &'a [u8]
    term: String
    // term: Term
}

fn create_test_index<'a>() ->  tantivy::Result<(Index, Field)> {
    // We first create a schema for the sake of the
    // example. Check the `basic_search` example for more information.
    let mut schema_builder = Schema::builder();

    // For this example, we need to make sure to index positions for our title
    // field. `TEXT` precisely does this.
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());

    let mut index_writer = index.writer_with_num_threads(1, 50_000_000)?;

    index_writer.add_document(doc!(title => "The Old Man and Mice the Sea"));
    index_writer.add_document(doc!(title => "Of Mice and Sea the"));
    index_writer.add_document(doc!(title => "web web web The modern Promotheus Mice"));

    index_writer.add_document(doc!(title => "internet web surfing"));
    index_writer.add_document(doc!(title => "internet surfing"));
    index_writer.add_document(doc!(title => "web surfing"));
    index_writer.add_document(doc!(title => "internet web surfing surfing beach"));
    index_writer.add_document(doc!(title => "surfing beach"));
    index_writer.add_document(doc!(title => "surfing beach"));

    index_writer.commit()?;
    Ok((index, title))
}

fn get_tantivy_matrix<'a>(index: Index, field: Field) ->  tantivy::Result<Vec<TantivyDocTermFreq>> {
    index.load_searchers()?;
    let searcher = index.searcher();
    let mut records = vec!();

    // A tantivy index is actually a collection of segments.
    // Similarly, a searcher just wraps a list `segment_reader`.
    //
    // (Because we indexed a very small number of documents over one thread
    // there is actually only one segment here, but let's iterate through the list
    // anyway)
    for segment_reader in searcher.segment_readers() {
        // A segment contains different data structure.
        // Inverted index stands for the combination of
        // - the term dictionary
        // - the inverted lists associated to each terms and their positions
        let inverted_index = segment_reader.inverted_index(field);


        // let terms = inverted_index.termdict.
        // println!("termdict {:?}", inverted_index.terms());
        let mut terms = inverted_index.terms().stream();
        // Iterate over the list of terms.
        while terms.advance() {
            // Get current term value.
            let current_term = terms.value();
            // Get current term as string.
            let current_text = str::from_utf8(terms.key()).unwrap().clone();

            // This segment posting object is like a cursor over the documents matching the term.
            // The `IndexRecordOption` arguments tells tantivy we will be interested in both term frequencies
            // and positions.
            //
            // If you don't need all this information, you may get better performance by decompressing less
            // information.
            let mut segment_postings =
                inverted_index.read_postings_from_terminfo(&current_term, IndexRecordOption::WithFreqsAndPositions);

                // this buffer will be used to request for positions
                // let mut positions: Vec<u32> = Vec::with_capacity(100);
                while segment_postings.advance() {
                    // the number of time the term appears in the document.
                    let doc_id: DocId = segment_postings.doc(); //< do not try to access this before calling advance once.

                    // This MAY contains deleted documents as well.
                    if segment_reader.is_deleted(doc_id) {
                        continue;
                    }

                    // the number of time the term appears in the document.
                    let term_freq: u32 = segment_postings.term_freq();

                    // accessing positions is slightly expensive and lazy, do not request
                    // for them if you don't need them for some documents.
                    // segment_postings.positions(&mut positions);

                    // By definition we should have `term_freq` positions.
                    // assert_eq!(positions.len(), term_freq as usize);

                    // Add a record with doc id, term, and term freq.
                    let record = TantivyDocTermFreq {
                        doc_id,
                        term_freq,
                        term: String::from(current_text)
                    };
                    records.push(record);
                }
        }
    }

    Ok(records)
}
