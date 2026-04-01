#!/usr/bin/env python3
import sys
from pathlib import Path
from ete4 import PhyloTree
from ete4.smartview import Layout
import re
import os
from Bio import SeqIO
from Bio import AlignIO
import shutil
from Bio.Align import MultipleSeqAlignment

'''
(c) Code written by Nikita Kulikov(1), Iker Irisarri(2); 
1) Leibniz Institute for the Analysis of Biodiversity Change (LIB)
2) Museo Nacional de Ciencias Naturales-CSIC (MNCN-CSIC)

This code loop through gene trees and determine speciation and duplication events. Then it removes inparalogous and outparalogous from the 
initial alignment, saving removed sequences in a separated file. It saves a phylogenetic tree in an extended newick format with the info
about speciation/duplication events. 


usage: python3 Paralogous_filtering.py  directory_with_trees directory_with_-corresponding_alignments

2025
'''

# First argument for a tree directory, second argument for a alignment directory
tree_file = sys.argv[1]
alignment_file = sys.argv[2]
alignment_out = sys.argv[3]
paralog_seqs = sys.argv[4]
paralog_stats = sys.argv[5]

def modify_alignment_headers(file_path):
    temp_file = file_path + ".tmp"
    with open(temp_file, "w") as out_handle:
        for record in SeqIO.parse(file_path, "fasta"):
            record.id = record.id.replace("+", "_")
            record.description = record.id
            SeqIO.write(record, out_handle, "fasta")

    #os.replace(temp_file, file_path)

def create_output_folders():
    main_folder = "paralogy_output"

    subfolders = [
        "paralogs_free_alignments",
        "extended_trees",
        "removed_paralogs",
    ]

    for folder in subfolders:
        path = os.path.join(main_folder, folder)
        os.makedirs(path, exist_ok=True)

def count_all_sequences(aln_path):
    return sum(1 for _ in SeqIO.parse(aln_path, "fasta"))

def extract_int_node_names(node):
    '''
    Rename nodes keeping just species names
    :param node: each node of the tree
    :return: renamed node
    '''
    # If the node has Homo_sapiens format
    if re.fullmatch(r"[A-Za-z]+_[A-Za-z]+(_[A-Za-z]+)?", node.name):
        return node.name
    # if not
    name_parts = node.name.split('|')[0].split('_')
    if bool(re.search(r'\d+at\d+', name_parts[2])):
        my_node = '_'.join(name_parts[:2])
    else:
        my_node = '_'.join(name_parts[:3])

    return my_node

def load_and_root_tree(tfile, afile=None):
    '''
    Load and root a tree
    :param tfile: a tree file in a newick format
    :return: loaded and rooted tree
    '''
    if os.path.getsize(tfile) == 0:
        print(f"Skipping tree {tfile}: empty treefile")
        return None
    t = PhyloTree(tfile, parser=1, sp_naming_function=extract_int_node_names)
    # Check number of taxa
    if len(list(t.leaves())) < 3:
        print(f"Skipping tree {tfile}: only {len(list(t.leaves()))} taxa")
        return None

    if afile:
        t.link_to_alignment(afile)


    # Root the gene tree to midpoint
    out = t.get_midpoint_outgroup()
    if out is not t:
        t.set_outgroup(out)

    t.standardize()
    return t

def deal_with_inparalogs(my_inpar,al_f):
    '''
    From a provided list of inparalog sequences keeps the longest sequence only. It also creates a file with deleted
    inparaloug sequences.
    :param my_inpar, al_f: list of all inparalogousss sequences, alignment file
    :return: nothing
    '''
    records = list(SeqIO.parse(al_f, "fasta"))
    #print(records)
    #print(my_inpar)
    target_records = [rec for rec in records if rec.id in my_inpar]
    other_records = [rec for rec in records if rec.id not in my_inpar]
    removed_records = []
    base_name, ext = os.path.splitext(al_f)
    removed_file = f"{base_name}.removed"
    #print(target_records)
    if target_records:
        longest_target = max(target_records, key=lambda r: len(r.seq.replace("-","").replace("X","")))# maybe not always the case
        removed_records = [rec for rec in target_records if rec.id != longest_target.id]
        final_records = other_records + [longest_target]

        #SeqIO.write(final_records, aoutfile, "fasta")
    num_removed = 0

    #if removed_records:
    #    with open(poutfile, "a") as rem_f:
    #        num_removed = SeqIO.write(removed_records, rem_f, "fasta")
    
    #return num_removed         
    return removed_records         


def count_number_of_sp(child):
    '''
    This function extracts set of species from the outparalogous branches
    :param child: one of the branches
    :return: set of species of this branch
    '''
    processed_nodes = set()
    for node_name in child:
        # If the alignment has Homo_sapiens format
        if re.fullmatch(r"[A-Za-z]+_[A-Za-z]+(_[A-Za-z]+)?", node_name):
            processed_nodes.add(node_name)  # e.g. "Homo_sapiens"
            continue
        # if not
        name_parts = node_name.split('|')[0].split('_')
        if len(name_parts) > 2 and re.search(r'\d+at\d+', name_parts[2]):
            my_node = '_'.join(name_parts[:2])
        else:
            my_node = '_'.join(name_parts[:3])
        processed_nodes.add(my_node)

    return processed_nodes

def keep_on_branch(child1,child2):
    '''
    This function extracts set of species from the outparalogous branches
    :param child1, child2: two branches with outparalogous
    :return: which branch has more species
    '''
    num_of_sp1 = count_number_of_sp(child1)
    num_of_sp2 = count_number_of_sp(child2)

    if len(num_of_sp1) > len(num_of_sp2):
        return "child1"
    else:
        return "child2"

def deal_with_outparalogs(child1,child2,alignment_filename):
    '''
    From provided branches with outparaloug sequences keeps the branch with the highest number of sequences. It also creates a file with deleted
    outparaloug sequences.
    :param child1, child2, alignment_filename: first child branch of  a duplication event, second branch, alignment file
    :return: nothing
    '''
#    print("Enter function")
    which_br_to_keep = keep_on_branch(child1,child2)
    alignment = AlignIO.read(alignment_filename, "fasta")
#    print("Which branch: ", which_br_to_keep)
    removed_records = []
    base_name, ext = os.path.splitext(alignment_filename)
    removed_file = f"{base_name}.removed"
    if which_br_to_keep == "child1":
        filtered_records = [record for record in alignment if record.id not in child2]
        removed_records = [record for record in alignment if record.id in child2]
    else:
        filtered_records = [record for record in alignment if record.id not in child1]
        removed_records = [record for record in alignment if record.id in child1]
    print("Removed records: ", removed_records)
    filtered_alignment = MultipleSeqAlignment(filtered_records)

    #AlignIO.write(filtered_alignment, aoutfile, "fasta")
    num_removed = 0
    #if removed_records:
    #    with open(poutfile, "a") as rem_f:
    #        num_removed = SeqIO.write(removed_records, rem_f, "fasta")
    return removed_records        


def detect_duplication_events(t,al_f,aoutfile,poutfile):
    '''
    Modifies a tree by determining duplication and speciation events . It also call for functions to remove in- and outparalogous from alignments
    :param t, al_f: tree file, alignment file
    :return: nothing
    '''
    events = t.get_descendant_evol_events()

    inparalogs_removed_per_gene = 0
    outparalogs_removed_per_gene = 0
    paralogs_removed=[]
    for ev in events:

        r = {'S': 'Orthology', 'D': 'Paralogy'}[ev.etype]

        if ev.etype == "D":
            print("We found a paralogy!")

            first_br = count_number_of_sp(ev.in_seqs)
            second_br = count_number_of_sp(ev.out_seqs)

            if len(first_br) == 1 and first_br == second_br:
                print("Inparalogy was detected!")
                my_inparalogy = ev.in_seqs.union(ev.out_seqs)
                print(my_inparalogy)
                #inparalogs_removed_per_gene += deal_with_inparalogs(my_inparalogy,al_f,aoutfile,poutfile)
                paralogs_removed.extend(deal_with_inparalogs(my_inparalogy,al_f))
                inparalogs_removed_per_gene+=len(deal_with_inparalogs(my_inparalogy,al_f))
            else:
                print("Outparalogy was detected!")
                print(ev.in_seqs)
                print(ev.out_seqs)
                #outparalogs_removed_per_gene += deal_with_outparalogs(ev.in_seqs,ev.out_seqs,aoutfile,poutfile)
                paralogs_removed.extend(deal_with_outparalogs(ev.in_seqs,ev.out_seqs,al_f))
                outparalogs_removed_per_gene+=len(deal_with_outparalogs(ev.in_seqs,ev.out_seqs,al_f))

        else:

            print("Orthology was detected!")
    remove_ids=[]
    for seq in paralogs_removed:
       remove_ids.append(seq.id) 
    records = list(SeqIO.parse(al_f, "fasta"))
    unique_records = [rec for rec in records if rec.id not in remove_ids]
    SeqIO.write(unique_records, aoutfile, "fasta")
    paralog_records = [rec for rec in records if rec.id in remove_ids]
    SeqIO.write(paralog_records, poutfile, "fasta")

    return inparalogs_removed_per_gene, outparalogs_removed_per_gene

def view_tree(tfile, afile, aoutfile, poutfile, soutfile):
    #create_output_folders()
    # get the lists of all files in the directories
    ## tree_list = os.listdir(tfile)
    ## alignment_list = os.listdir(afile)
    total_seq_numb = 0
    total_inparalogs_removed = 0
    total_outparalogs_removed = 0
    ## for tree_file in tree_list:
    tree_file=tfile
    # look for the treefile extension
    #if tree_file.endswith(".treefile"): # efile"):
    ## for alignment_file in alignment_list:
    alignment_file=afile
        # check if the alignment file corresponds to the tree file
        #tree_file_check = tree_file.split(".")[0].split("_")
        #tree_file_check_str =  '_'.join(tree_file_check[:2])
        #alignment_file_check = alignment_file.split(".")[0].split("_")
        #alignment_file_check_str =  '_'.join(alignment_file_check[:2])
        #if alignment_file_check_str == tree_file_check_str:
        #print(tree_file_check_str)
        #tree_file = os.path.join(tfile, tree_file)
        #alignment_file = os.path.join(afile, alignment_file)
    modify_alignment_headers(alignment_file)
    alignment_file=afile + '.tmp'
        # load and root trees to a midpoint using ete4
    t = load_and_root_tree(tree_file, alignment_file)
    if t is None:
        open(aoutfile, 'w').close()
        open(poutfile, 'w').close()
        with open(soutfile, 'w') as par_stat:
            par_stat.write(f"Name\tSequence_number\tIn_paralogs\tOut_paralogs\tIn_paralog_proportion\tOut_paralog_proportion\n")
            par_stat.write(f"skipped\t0\t0\t0\t0\t0\n")
        return
        # preserve the original alignment file
    base_name, ext = os.path.splitext(alignment_file)
    #alignment_file_copy = f"{base_name}.original"
    #shutil.copy(alignment_file, alignment_file_copy)
        # detecte speciation/duplication events and remove in- and outparalogous from the alignment
    inpar_gene, out_par_gene = detect_duplication_events(t,alignment_file,aoutfile,poutfile)
        # save extended newick trees with the duplication/speciation info
        #output_dir = Path("paralogy_output") / "extended_trees"
        #base_name = Path(tree_file).stem
        ## replace with input filename above
    #output_path = aoutfile

    #t.write(toutfile,props=['evoltype'])

        ### here we are calculating paralogy statistic ###
        # count all sequences in the alignments
    seq_numb = count_all_sequences(alignment_file)
    total_seq_numb += seq_numb
        # count all in/outparalogs
    total_inparalogs_removed += inpar_gene
    total_outparalogs_removed += out_par_gene
        # count in/outparalogs per gene
        #with open("paralogy_output/{base_name}_paralogy_statistic_per_gene.txt", "a") as par_stat:
    with open(soutfile, "a") as par_stat:
        inpar_prop = inpar_gene/seq_numb
        outpar_prop = out_par_gene/seq_numb
        par_stat.write(f"Name\tSequence_number\tIn_paralogs\tOut_paralogs\tIn_paralog_proportion\tOut_paralog_proportion\n")
        par_stat.write(f"{base_name}\t{seq_numb}\t{inpar_gene}\t{out_par_gene}\t{inpar_prop}\t{outpar_prop}\n")
        # move file to the output directory
        ## Can probably remove this, just use the input filenames
        #source_file = alignment_file
        #dest_dir = "paralogy_output/{base_name}_paralogs_free_alignments"
        #filename = os.path.basename(source_file)
        #dest_file = os.path.join(dest_dir, filename)
        #shutil.move(source_file, dest_file)
    ## I would suggest removing this section
    #with open("paralogy_output/{base_name}_paralogy_summary.txt", "a") as par_sum:
    #    prop_inp = total_inparalogs_removed/total_seq_numb
    #    prop_out = total_outparalogs_removed/total_seq_numb
    #    par_sum.write(f"Paralogy summary\n")
    #    par_sum.write(f"Total number of sequences\tTotal number of inparalogs\tTotal number of outparalogs\tProportion of inparalogs\tProportion of outparalogs\n")
    #    par_sum.write(f"{total_seq_numb}\t{total_inparalogs_removed}\t{total_outparalogs_removed}\t{prop_inp}\t{prop_out}\n")

#excute only if called from the command line
if __name__ == '__main__':
        view_tree(sys.argv[1], sys.argv[2], sys.argv[3],sys.argv[4],sys.argv[5])

