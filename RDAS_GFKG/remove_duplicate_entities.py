import scispacy
import spacy
from spacy.language import Language


@Language.component('remove_duplicate_entities')
def remove_duplicate_entities(doc):
    '''
    Remove duplicate entities detected by scispacy
    '''
    
    unique_ents_text = []
    remove_ents_list = []
    
    ents = list(doc.ents)
 
    for ent in ents:
        if ent.text in unique_ents_text:
            remove_ents_list.append(ent)
        else:
            unique_ents_text.append(ent.text)
            
    for ent in remove_ents_list:
        ents.remove(ent)
    
    doc.ents = tuple(ents)
    return (doc)
Language.component('remove_duplicate_entities', func=remove_duplicate_entities)

