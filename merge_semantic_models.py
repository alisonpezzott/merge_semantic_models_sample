import json
import os
import pyfabricops as pf
import re
import shutil


from glob import glob
from pathlib import Path


pf.set_auth_provider('oauth')
pf.setup_logging(level='info', format_style='minimal')


def merge_model_tmdl(direct_lake_content, import_content):
    """Merge model.tmdl files by combining ref table statements"""
    lines = direct_lake_content.strip().split('\n')
    
    # Extract ref table statements from import content
    import_ref_tables = []
    for line in import_content.split('\n'):
        line = line.strip()
        if line.startswith('ref table '):
            table_name = line.replace('ref table ', '')
            import_ref_tables.append(table_name)
    
    # Add new ref table statements before the last line (which should be empty or last statement)
    result_lines = []
    ref_table_section_started = False
    
    for i, line in enumerate(lines):
        if line.strip().startswith('ref table '):
            ref_table_section_started = True
            result_lines.append(line)
        elif ref_table_section_started and line.strip() == '':
            # Add import ref tables here
            for table_name in import_ref_tables:
                # Check if table is not already referenced
                table_already_exists = any(
                    existing_line.strip() == f'ref table {table_name}'
                    for existing_line in lines
                )
                if not table_already_exists:
                    result_lines.append(f'ref table {table_name}')
            result_lines.append(line)
            ref_table_section_started = False
        else:
            result_lines.append(line)
    
    # If no empty line was found after ref tables, add the import tables at the end
    if ref_table_section_started:
        for table_name in import_ref_tables:
            table_already_exists = any(
                existing_line.strip() == f'ref table {table_name}'
                for existing_line in lines
            )
            if not table_already_exists:
                result_lines.append(f'ref table {table_name}')
    
    return '\n'.join(result_lines)

def merge_relationships_tmdl(direct_lake_content, import_content):
    """Merge relationships.tmdl files by combining all relationships"""
    # Simply concatenate the content, removing duplicate relationships if any
    combined_content = direct_lake_content.strip() + '\n\n' + import_content.strip()
    return combined_content

def merge_expressions_tmdl(direct_lake_content, import_content):
    """Merge expressions.tmdl files by combining all expressions"""
    # Simply concatenate the content
    combined_content = direct_lake_content.strip() + '\n\n' + import_content.strip()
    return combined_content

def copy_directory_recursive(src_dir, dst_dir, merge_files=True):
    """Recursively copy directory, merging files when needed"""
    if not src_dir.exists():
        return
        
    dst_dir.mkdir(parents=True, exist_ok=True)
    
    for item in src_dir.iterdir():
        dst_item = dst_dir / item.name
        
        if item.is_dir():
            copy_directory_recursive(item, dst_item, merge_files)
        elif item.is_file():
            if dst_item.exists() and merge_files:
                # File exists, don't overwrite - Import tables should not overwrite DirectLake tables
                continue
            else:
                shutil.copy2(item, dst_item)

def merge_semantic_models(
        direct_lake_model_path,
        import_model_path, 
        output_path, 
        merged_semantic_model_name,
    ):

    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)

    # Copy definition.pbism from DirectLake
    definition_pbism_path = Path(direct_lake_model_path) / "definition.pbism"
    output_pbism_path = Path(output_path) / "definition.pbism"
    shutil.copy(definition_pbism_path, output_pbism_path)

    # Copy .platform setting the new displayName
    platform_path = Path(direct_lake_model_path) / ".platform"
    output_platform_path = Path(output_path) / ".platform"

    if platform_path.exists():
        with open(platform_path) as f:
            platform = json.load(f)

        platform['metadata']['displayName'] = merged_semantic_model_name
        
        with open(output_platform_path, 'w') as f:
            json.dump(platform, f, indent=2)
    
    # Merge all files into definition folder
    import_definition_path = Path(import_model_path) / "definition"
    direct_lake_definition_path = Path(direct_lake_model_path) / "definition" 
    merged_definition_path = Path(output_path) / "definition"

    # Create merged definition directory if it doesn't exist
    os.makedirs(merged_definition_path, exist_ok=True)

    # First, copy all files from direct lake definition to merged definition folder
    for file_path in direct_lake_definition_path.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(direct_lake_definition_path)
            target_path = merged_definition_path / relative_path
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file_path, target_path)

    # Then, merge content from import definition
    for file_path in import_definition_path.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(import_definition_path)
            target_path = merged_definition_path / relative_path
            
            if target_path.exists():
                # File exists, need to merge content
                with open(target_path, 'r', encoding='utf-8') as f:
                    existing_content = f.read()
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    import_content = f.read()
                
                merged_content = existing_content
                
                if file_path.name == 'model.tmdl':
                    merged_content = merge_model_tmdl(existing_content, import_content)
                elif file_path.name == 'relationships.tmdl':
                    merged_content = merge_relationships_tmdl(existing_content, import_content)
                elif file_path.name == 'expressions.tmdl':
                    merged_content = merge_expressions_tmdl(existing_content, import_content)
                elif file_path.suffix == '.tmdl' and file_path.parent.name == 'tables':
                    # For table files, keep the DirectLake version (don't overwrite)
                    continue
                # For other files, keep the DirectLake version by default
                
                with open(target_path, 'w', encoding='utf-8') as f:
                    f.write(merged_content)
            else:
                # File doesn't exist in target, copy it
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(file_path, target_path)
    
    print(f"Merged semantic models successfully created at: {output_path}")

def export_source_semantic_models(
        workspace,
        direct_lake_semantic_model,
        import_semantic_model,
        export_path
):
    pf.export_semantic_model(workspace, direct_lake_semantic_model, export_path)
    pf.export_semantic_model(workspace, import_semantic_model, export_path)  

def export_merge_and_deploy(
        workspace,
        direct_lake_semantic_model_name,
        import_semantic_model_name,
        merged_semantic_model_name,
        source_path,
        output_path,
):
    export_source_semantic_models(
        workspace = workspace, # or name
        direct_lake_semantic_model = direct_lake_semantic_model_name, # or ID
        import_semantic_model = import_semantic_model_name, # or ID
        export_path = source_path
    )

    merge_semantic_models(
        direct_lake_model_path = Path(source_path) / f'{direct_lake_semantic_model_name}.SemanticModel',
        import_model_path = Path(source_path) / f'{import_semantic_model_name}.SemanticModel',
        output_path = Path(output_path) / f'{merged_semantic_model_name}.SemanticModel',
        merged_semantic_model_name = merged_semantic_model_name,
    )

    pf.deploy_semantic_model(
        workspace = workspace, # or name
        path = f'{output_path}/{merged_semantic_model_name}.SemanticModel',
    )

if __name__ == "__main__":
    
    export_merge_and_deploy(
        workspace = '9f06a268-59d7-498a-bb6f-a15dc6de53bd', # or name
        direct_lake_semantic_model_name = 'DirectLake',
        import_semantic_model_name = 'Import',
        merged_semantic_model_name = 'CompositeDL',
        source_path = 'Source',
        output_path = 'Merged',
    )
