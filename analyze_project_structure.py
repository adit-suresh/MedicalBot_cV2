"""
Script to analyze project structure, identify unused files, and suggest refactoring opportunities.
"""
import os
import sys
import re
import importlib
import inspect
from collections import defaultdict
import ast

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

class CodeAnalyzer:
    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.py_files = []
        self.imports = defaultdict(set)
        self.imported_by = defaultdict(set)
        self.class_locations = {}
        self.function_locations = {}
        self.class_usages = defaultdict(set)
        self.function_usages = defaultdict(set)
        self.duplicate_files = []
        self.similar_files = []

    def find_all_python_files(self):
        """Find all Python files in the project."""
        for root, dirs, files in os.walk(self.root_dir):
            for file in files:
                if file.endswith('.py'):
                    rel_path = os.path.relpath(os.path.join(root, file), self.root_dir)
                    self.py_files.append(rel_path)
        
        print(f"Found {len(self.py_files)} Python files")
        return self.py_files

    def analyze_imports(self):
        """Analyze imports across all Python files."""
        for file_path in self.py_files:
            try:
                with open(os.path.join(self.root_dir, file_path), 'r', encoding='utf-8') as f:
                    source = f.read()
                
                tree = ast.parse(source)
                imports = set()
                
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for name in node.names:
                            imports.add(name.name)
                    elif isinstance(node, ast.ImportFrom):
                        if node.module:
                            imports.add(node.module)
                
                self.imports[file_path] = imports
                
                # Track imported_by
                for imported in imports:
                    # Convert import to potential file path patterns
                    parts = imported.split('.')
                    if parts[0] in ['src', 'config', 'tests']:
                        potential_file = '/'.join(parts) + '.py'
                        self.imported_by[potential_file].add(file_path)
                        
                        # Also add without src/ prefix
                        if parts[0] == 'src':
                            alt_path = '/'.join(parts[1:]) + '.py'
                            self.imported_by[alt_path].add(file_path)
                            
            except Exception as e:
                print(f"Error analyzing imports in {file_path}: {str(e)}")
        
        return self.imports

    def find_unused_files(self):
        """Find files that aren't imported anywhere."""
        unused_files = []
        main_files = []
        test_files = []
        
        for file_path in self.py_files:
            # Skip test files from being marked unused, but track them separately
            if file_path.startswith('tests/') or 'test_' in file_path:
                test_files.append(file_path)
                continue
                
            # Skip example/documentation files
            if re.search(r'example|demo|__init__|__main__|conftest', os.path.basename(file_path)):
                continue
                
            # Check if the file has a main block (runnable script)
            try:
                with open(os.path.join(self.root_dir, file_path), 'r', encoding='utf-8') as f:
                    content = f.read()
                    if re.search(r'if\s+__name__\s*==\s*[\'"]__main__[\'"]\s*:', content):
                        main_files.append(file_path)
                        continue
            except:
                pass
                
            # Calculate normalized path variations to check imports
            path_variations = [
                file_path,
                file_path.replace('/', '.').replace('.py', ''),  # src/utils/file.py -> src.utils.file
                '.'.join(file_path.split('/')[1:]).replace('.py', '')  # src/utils/file.py -> utils.file
            ]
            
            # Check if file is imported somewhere
            imported = False
            for path_var in path_variations:
                for imports in self.imports.values():
                    if path_var in imports:
                        imported = True
                        break
                if imported:
                    break
                    
            # Check imported_by
            if not imported and file_path not in self.imported_by:
                unused_files.append(file_path)
        
        return {
            'unused_files': unused_files,
            'main_files': main_files,
            'test_files': test_files
        }

    def find_duplicate_content(self):
        """Find files with duplicate or very similar content."""
        file_hashes = {}
        file_content = {}
        
        for file_path in self.py_files:
            try:
                with open(os.path.join(self.root_dir, file_path), 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                # Normalize content by removing comments and whitespace
                clean_content = re.sub(r'#.*$', '', content, flags=re.MULTILINE)
                clean_content = re.sub(r'\s+', ' ', clean_content).strip()
                
                # Save both original and cleaned content
                file_content[file_path] = {
                    'original': content,
                    'clean': clean_content
                }
                
                # Hash the clean content
                content_hash = hash(clean_content)
                
                if content_hash in file_hashes:
                    self.duplicate_files.append((file_path, file_hashes[content_hash]))
                else:
                    file_hashes[content_hash] = file_path
                    
            except Exception as e:
                print(f"Error analyzing content in {file_path}: {str(e)}")
        
        # Find similar (not identical) files
        for file1, content1 in file_content.items():
            for file2, content2 in file_content.items():
                if file1 == file2 or (file1, file2) in self.similar_files or (file2, file1) in self.similar_files:
                    continue
                    
                # Skip files in different main directories
                if file1.split('/')[0] != file2.split('/')[0]:
                    continue
                
                # Check similarity
                similarity = self._calculate_similarity(content1['clean'], content2['clean'])
                if similarity > 0.7:  # 70% similar
                    self.similar_files.append((file1, file2, similarity))
        
        return {
            'duplicates': self.duplicate_files,
            'similar': sorted(self.similar_files, key=lambda x: x[2], reverse=True)
        }
    
    def _calculate_similarity(self, str1, str2):
        """Calculate Jaccard similarity between two strings."""
        set1 = set(str1.split())
        set2 = set(str2.split())
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0
    
    def find_refactoring_opportunities(self):
        """Identify files that might benefit from refactoring."""
        large_files = []
        complex_files = []
        deprecated_patterns = []
        
        for file_path in self.py_files:
            try:
                with open(os.path.join(self.root_dir, file_path), 'r', encoding='utf-8') as f:
                    content = f.read()
                    lines = content.split('\n')
                
                # Check file size
                if len(lines) > 300:
                    large_files.append((file_path, len(lines)))
                
                # Check file complexity (functions with many lines)
                tree = ast.parse(content)
                for node in ast.walk(tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        if node.end_lineno - node.lineno > 50:
                            complex_files.append((file_path, node.name, node.end_lineno - node.lineno))
                
                # Check for deprecated patterns
                patterns = [
                    (r'OCRmyPDF', 'Referenced in timeline but replaced by Textract/DeepSeek'),
                    (r'PySpark', 'Referenced in timeline but not used in codebase'),
                    (r'\.submit\(\)', 'Old portal submission code'),
                    (r'ClaudeProcessor', 'Not yet integrated with DeepSeek')
                ]
                
                for pattern, reason in patterns:
                    if re.search(pattern, content):
                        deprecated_patterns.append((file_path, pattern, reason))
                
            except Exception as e:
                print(f"Error analyzing file {file_path}: {str(e)}")
        
        return {
            'large_files': large_files,
            'complex_functions': complex_files,
            'deprecated_patterns': deprecated_patterns
        }

    def run_analysis(self):
        """Run all analysis steps."""
        self.find_all_python_files()
        self.analyze_imports()
        unused = self.find_unused_files()
        duplicates = self.find_duplicate_content()
        refactoring = self.find_refactoring_opportunities()
        
        return {
            'unused_files': unused,
            'duplicates': duplicates,
            'refactoring': refactoring
        }


def calculate_test_coverage():
    """Calculate test coverage for project files."""
    test_files = []
    source_files = []
    coverage = {}
    
    # Find all test files
    for root, dirs, files in os.walk(project_root):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.relpath(os.path.join(root, file), project_root)
                
                if file.startswith('test_') or '/test_' in file_path or file_path.startswith('tests/'):
                    test_files.append(file_path)
                elif not file.startswith('_') and file_path.startswith('src/'):
                    source_files.append(file_path)
    
    # Analyze test coverage
    for source_file in source_files:
        module_name = source_file.replace('/', '.').replace('.py', '')
        tested = False
        
        # Check for direct test file match
        test_file = f"test_{os.path.basename(source_file)}"
        test_file2 = f"tests/{source_file.replace('src/', '')}"
        test_file3 = f"tests/test_{source_file.replace('src/', '')}"
        
        if any(test == test_file for test in test_files) or \
           any(test == test_file2 for test in test_files) or \
           any(test == test_file3 for test in test_files):
            tested = True
        
        # Check if imported in any test file
        if not tested:
            for test_file in test_files:
                try:
                    with open(os.path.join(project_root, test_file), 'r', encoding='utf-8') as f:
                        content = f.read()
                        # Check if the source file's module is imported
                        if module_name in content or os.path.basename(source_file).replace('.py', '') in content:
                            tested = True
                            break
                except:
                    pass
        
        coverage[source_file] = tested
    
    # Calculate coverage percentage
    covered = sum(1 for is_tested in coverage.values() if is_tested)
    total = len(coverage)
    coverage_pct = (covered / total * 100) if total > 0 else 0
    
    return {
        'coverage': coverage,
        'summary': {
            'total_source_files': total,
            'covered_files': covered,
            'coverage_percentage': coverage_pct
        }
    }


def main():
    """Run the project structure analysis."""
    print(f"Analyzing project structure in {project_root}...")
    analyzer = CodeAnalyzer(project_root)
    results = analyzer.run_analysis()
    
    coverage = calculate_test_coverage()
    
    # Print report
    print("\n" + "="*80)
    print("PROJECT STRUCTURE ANALYSIS REPORT".center(80))
    print("="*80)
    
    # Unused files
    print("\n1. POTENTIALLY UNUSED FILES")
    print("-"*80)
    unused = results['unused_files']['unused_files']
    if unused:
        for file in unused:
            print(f"  - {file}")
    else:
        print("  No unused files found!")
    
    # Duplicates
    print("\n2. DUPLICATE/SIMILAR FILES")
    print("-"*80)
    duplicates = results['duplicates']['duplicates']
    similar = results['duplicates']['similar']
    
    if duplicates:
        print("2.1 Exact duplicates:")
        for file1, file2 in duplicates:
            print(f"  - {file1} is duplicate of {file2}")
    else:
        print("  No exact duplicates found!")
        
    if similar:
        print("\n2.2 Similar files (may need consolidation):")
        for file1, file2, similarity in similar[:5]:  # Show top 5
            print(f"  - {file1} is {similarity:.0%} similar to {file2}")
    
    # Refactoring opportunities
    print("\n3. REFACTORING OPPORTUNITIES")
    print("-"*80)
    
    large_files = results['refactoring']['large_files']
    if large_files:
        print("3.1 Large files (>300 lines):")
        for file, line_count in sorted(large_files, key=lambda x: x[1], reverse=True)[:5]:
            print(f"  - {file}: {line_count} lines")
    
    complex_funcs = results['refactoring']['complex_functions']
    if complex_funcs:
        print("\n3.2 Complex functions (>50 lines):")
        for file, func, line_count in sorted(complex_funcs, key=lambda x: x[2], reverse=True)[:5]:
            print(f"  - {file}: {func}() - {line_count} lines")
    
    deprecated = results['refactoring']['deprecated_patterns']
    if deprecated:
        print("\n3.3 Deprecated patterns:")
        for file, pattern, reason in deprecated:
            print(f"  - {file}: contains '{pattern}' ({reason})")
    
    # Test coverage
    print("\n4. TEST COVERAGE")
    print("-"*80)
    summary = coverage['summary']
    print(f"Overall coverage: {summary['coverage_percentage']:.1f}% ({summary['covered_files']}/{summary['total_source_files']} files)")
    
    print("\n4.1 Untested files:")
    untested_files = [file for file, is_tested in coverage['coverage'].items() if not is_tested]
    for file in sorted(untested_files)[:10]:  # Show first 10
        print(f"  - {file}")
    if len(untested_files) > 10:
        print(f"  ... and {len(untested_files) - 10} more")
    
    # Recommendations
    print("\n" + "="*80)
    print("RECOMMENDATIONS".center(80))
    print("="*80)
    
    # Files to delete
    print("\n1. Files that could be removed:")
    delete_candidates = [f for f in unused if "test_" not in f and not f.startswith("test")]
    if delete_candidates:
        for file in delete_candidates:
            print(f"  - {file}")
    else:
        print("  No clear candidates for removal")
    
    # Files to refactor
    print("\n2. Priority refactoring targets:")
    refactor_targets = []
    
    # Add files with deprecated patterns
    for file, pattern, reason in deprecated:
        refactor_targets.append((file, f"Contains deprecated pattern: {pattern}"))
    
    # Add complex files that need refactoring
    for file, lines in large_files:
        if file in [f for f, _, _ in complex_funcs]:
            refactor_targets.append((file, f"Large file ({lines} lines) with complex functions"))
    
    # Add similar files that should be consolidated
    similar_to_refactor = [
        (file1, f"Very similar to {file2} ({similarity:.0%})")
        for file1, file2, similarity in similar if similarity > 0.8
    ]
    refactor_targets.extend(similar_to_refactor)
    
    # Show unique files
    shown_files = set()
    for file, reason in refactor_targets:
        if file not in shown_files:
            print(f"  - {file}: {reason}")
            shown_files.add(file)
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()