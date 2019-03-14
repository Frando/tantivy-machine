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
use tantivy::schema::*;
use tantivy::Index;
use tantivy::{DocId, DocSet, Postings};

use std::collections::HashMap;

use rulinalg::matrix::{BaseMatrixMut, MatrixSliceMut, Matrix};
use std::str;

use std::fmt;

fn main() -> tantivy::Result<()> {

    // Get result rows from tantivy.
    let tantivy_result = get_tantivy_matrix()?;

    // Calculate number of documents.
    let mut max_docid = 0;
    for record in &tantivy_result {
        if record.doc_id > max_docid {
            max_docid = record.doc_id;
        }
    }

    // Map terms (strings) to ids (ints).
    let mut terms_map = HashMap::new();
    let mut max_termid = 0;
    for record in &tantivy_result {
        if let Some(_) = terms_map.get(&record.term) {
        } else {
            terms_map.insert(record.term.clone(), max_termid);
            max_termid = max_termid + 1;
        }
    }

    println!("terms map {:?}", terms_map);
    println!("max_docid {:?}", max_docid);
    println!("max_termid{:?}", max_termid);

    // Create zeroed matrix with docs as rows and terms as columns.
    let mut a = Matrix::<u32>::zeros(max_docid as usize + 1, max_termid + 1);

    // Iterate over result rows.
    for record in &tantivy_result {
        let term_index = terms_map.get(&record.term).unwrap();
        println!("record {:?} {:?} {:?}", record.doc_id, *term_index, record.term_freq);

        // Set value in matrix.
        let mut slice = a.sub_slice_mut([record.doc_id as usize, *term_index], 1, 1);
        let mut value = slice.iter_mut().next().unwrap();
        *value = record.term_freq;
    }
    println!("matrix {:?}", a);

    Ok(())
}

#[derive(Debug)]
struct TantivyDocTermFreq {
    doc_id: DocId,
    term_freq: u32,
    // text: &'a [u8]
    term: String
    // term: Term
}

fn get_tantivy_matrix<'a>() ->  tantivy::Result<Vec<TantivyDocTermFreq>> {
    // We first create a schema for the sake of the
    // example. Check the `basic_search` example for more information.
    let mut schema_builder = Schema::builder();

    // For this example, we need to make sure to index positions for our title
    // field. `TEXT` precisely does this.
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());

    let mut index_writer = index.writer_with_num_threads(1, 50_000_000)?;
    index_writer.add_document(doc!(title => "The Old Man and the Sea"));
    index_writer.add_document(doc!(title => "Of Mice and Men"));
    index_writer.add_document(doc!(title => "The modern Promotheus"));
    index_writer.commit()?;

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
        let inverted_index = segment_reader.inverted_index(title);


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
                let mut positions: Vec<u32> = Vec::with_capacity(100);
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
                    segment_postings.positions(&mut positions);

                    // By definition we should have `term_freq` positions.
                    assert_eq!(positions.len(), term_freq as usize);

                    // This prints:
                    // ```
                    // Doc 0: TermFreq 2: [0, 4]
                    // Doc 2: TermFreq 1: [0]
                    // ```
                    let record = TantivyDocTermFreq {
                        doc_id,
                        term_freq,
                        term: String::from(current_text)
                        // text: current_key
                    };
                    records.push(record);
                    // println!("Doc {}: TermFreq {}: {:?}", doc_id, term_freq, positions);
                }
        }
    }

    Ok(records)
}
