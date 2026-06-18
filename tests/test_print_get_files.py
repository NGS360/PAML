'''
Integration script that calls get_files() and get_files_fullpath() and prints
the details of everything returned, to help visualize the differences.

Creates a project with files at various depths, prints results, then cleans up.

Usage:
    INTEGRATION_TEST=1 python tests/test_print_get_files.py
'''
import os
import sys
import random
import string
from datetime import datetime

from cwl_platform import PlatformFactory


def generate_random_file(prefix=''):
    '''Generate a temp file with random content'''
    file_name = prefix + ''.join(random.choices(string.ascii_letters + string.digits, k=8)) + ".txt"
    with open(file_name, 'w', encoding='us-ascii') as f:
        f.write(''.join(random.choices(string.ascii_letters + string.digits, k=100)))
    return file_name


def print_separator(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def print_results(label, results):
    print(f"\n  {label} ({len(results)} results):")
    print(f"  {'Path':<50} {'Second element'}")
    print(f"  {'-'*50} {'-'*40}")
    for path, second in results:
        second_repr = repr(second)
        if len(second_repr) > 40:
            second_repr = second_repr[:37] + '...'
        print(f"  {path:<50} {second_repr}")


def main():
    if 'INTEGRATION_TEST' not in os.environ:
        print("Set INTEGRATION_TEST=1 to run this script against the live API.")
        sys.exit(1)

    project_name = f"TestPrintGetFiles-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    local_files = []

    platforms_to_test = ['Arvados', 'SevenBridges']

    for platform_name in platforms_to_test:
        print_separator(f"Platform: {platform_name}")

        platform = PlatformFactory().get_platform(platform_name)
        platform.connect()

        project = platform.create_project(project_name, 'Print get_files test')
        print(f"\n  Created project: {project_name}")

        try:
            # Upload files at various locations
            file_root = generate_random_file(prefix='root_')
            local_files.append(file_root)
            file_inputs = generate_random_file(prefix='input_')
            local_files.append(file_inputs)
            file_nested = generate_random_file(prefix='nested_')
            local_files.append(file_nested)

            print(f"\n  Uploading files...")
            print(f"    {file_root} -> / (root)")
            platform.upload_file(file_root, project)

            print(f"    {file_inputs} -> /inputs")
            platform.upload_file(file_inputs, project, dest_folder='/inputs')

            print(f"    {file_nested} -> /inputs/run1/sample1")
            platform.upload_file(file_nested, project, dest_folder='/inputs/run1/sample1')

            # Call get_files() with no filters
            print_separator(f"[{platform_name}] get_files(project) - no filters")
            files = platform.get_files(project)
            print_results("get_files()", files)

            # Call get_files_fullpath() with no filters
            print_separator(f"[{platform_name}] get_files_fullpath(project) - no filters")
            files_fullpath = platform.get_files_fullpath(project)
            print_results("get_files_fullpath()", files_fullpath)

            # Show type difference
            print_separator(f"[{platform_name}] Type comparison")
            if files:
                _, second_get_files = files[0]
                print(f"  get_files() second element type:          {type(second_get_files).__name__}")
                print(f"  get_files() second element repr:          {repr(second_get_files)[:80]}")
            if files_fullpath:
                _, second_fullpath = files_fullpath[0]
                print(f"  get_files_fullpath() second element type:  {type(second_fullpath).__name__}")
                print(f"  get_files_fullpath() second element repr:  {repr(second_fullpath)[:80]}")

            # With folder filter
            print_separator(f"[{platform_name}] get_files(project, folder='/inputs') - folder filter")
            files_filtered = platform.get_files(project, filters={'folder': '/inputs'})
            print_results("get_files(filters={'folder': '/inputs'})", files_filtered)

            print_separator(f"[{platform_name}] get_files_fullpath(project, folder='/inputs') - folder filter")
            files_fullpath_filtered = platform.get_files_fullpath(project, filters={'folder': '/inputs'})
            print_results("get_files_fullpath(filters={'folder': '/inputs'})", files_fullpath_filtered)

            # With prefix filter
            print_separator(f"[{platform_name}] get_files(project, prefix='nested_') - prefix filter")
            files_prefix = platform.get_files(project, filters={'prefix': 'nested_'})
            print_results("get_files(filters={'prefix': 'nested_'})", files_prefix)

            print_separator(f"[{platform_name}] get_files_fullpath(project, prefix='nested_') - prefix filter")
            files_fullpath_prefix = platform.get_files_fullpath(project, filters={'prefix': 'nested_'})
            print_results("get_files_fullpath(filters={'prefix': 'nested_'})", files_fullpath_prefix)

        finally:
            # Cleanup
            print(f"\n  Cleaning up project: {project_name}")
            platform.delete_project_by_name(project_name)

    # Remove local files
    for f in local_files:
        if os.path.exists(f):
            os.remove(f)

    print_separator("Done")
    print("  All projects and local files cleaned up.\n")


if __name__ == '__main__':
    main()
