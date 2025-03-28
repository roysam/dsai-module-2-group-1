# -*- coding: utf-8 -*- #
# Copyright 2023 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Command to Update a Cloud Security Command Center finding's mute state."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.scc.findings import util
from googlecloudsdk.generated_clients.apis.securitycenter.v1 import securitycenter_v1_messages as messages


@base.ReleaseTracks(base.ReleaseTrack.GA, base.ReleaseTrack.ALPHA)
class SetMute(base.Command):
  """Update a Cloud Security Command Center finding's mute state."""

  detailed_help = {
      "DESCRIPTION": (
          "Update a Cloud Security Command Center finding's mute state."
      ),
      "EXAMPLES": """
        To update finding's mute state to ``MUTED'', given finding organizations/123/sources/456/findings/789, run:

          $ {command} 789 --organization=organizations/123 --source=456 --mute=MUTED
          $ {command} 789 --organization=123 --source=456 --mute=MUTED
          $ {command} organizations/123/sources/456/findings/789 --mute=MUTED

        To update finding's mute state to ``UNMUTED'', given finding organizations/123/sources/456/findings/789, run:

          $ {command} 789 --organization=organizations/123 --source=456 --mute=UNMUTED
          $ {command} 789 --organization=123 --source=456 --mute=UNMUTED
          $ {command} organizations/123/sources/456/findings/789 --mute=UNMUTED

        To update finding's mute state to ``MUTED'', given finding folders/123/sources/456/findings/789, run:

          $ {command} 789 --folder=folders/123 --source=456 --mute=MUTED
          $ {command} 789 --folder=123 --source=456 --mute=MUTED
          $ {command} folders/123/sources/456/findings/789 --mute=MUTED

        To update finding's mute state to ``MUTED'', given finding projects/123/sources/456/findings/789, run:

          $ {command} 789 --project=projects/123 --source=456 --mute=MUTED
          $ {command} 789 --project=123 --source=456 --mute=MUTED
          $ {command} projects/123/sources/456/findings/789 --mute=MUTED""",
      "API REFERENCE": """
          This command uses the securitycenter/v1 API. The full documentation for
          this API can be found at: https://cloud.google.com/security-command-center""",
  }

  @staticmethod
  def Args(parser):
    # Add flags and finding positional argument.
    parser.add_argument(
        "finding",
        help="ID of the finding or the full resource name of the finding.",
    )
    source_group = parser.add_group(mutex=True)
    source_group.add_argument(
        "--organization",
        help="""Organization where the finding resides. Formatted as ``organizations/123'' or just ``123''.""",
    )

    source_group.add_argument(
        "--folder",
        help="""Folder where the finding resides. Formatted as ``folders/456'' or just ``456''.""",
    )
    source_group.add_argument(
        "--project",
        help="""Project (id or number) where the finding resides. Formatted as ``projects/789'' or just ``789''.""",
    )

    # To accept both lower and uppercase arguments for the choices we use
    # base.ChoiceArgument.
    base.ChoiceArgument(
        "--mute",
        required=True,
        default="mute_unspecified",
        choices=["muted", "unmuted"],
        help_str="Desired mute state of the finding.",
    ).AddToParser(parser)

    parser.add_argument("--source", help="ID of the source.")

  def Run(self, args):
    # Create and build the request.
    request = (
        messages.SecuritycenterOrganizationsSourcesFindingsSetMuteRequest()
    )
    request.setMuteRequest = messages.SetMuteRequest()
    set_mute_dict = {
        "mute_unspecified": (
            messages.SetMuteRequest.MuteValueValuesEnum.MUTE_UNSPECIFIED
        ),
        "muted": messages.SetMuteRequest.MuteValueValuesEnum.MUTED,
        "unmuted": messages.SetMuteRequest.MuteValueValuesEnum.UNMUTED,
        "undefined": messages.SetMuteRequest.MuteValueValuesEnum.UNDEFINED,
    }

    # The muted option has to be case insensitive so we convert to lower before
    # mapping to set_mute_dict.
    args.mute = args.mute.lower()
    request.setMuteRequest.mute = set_mute_dict.get(
        args.mute, messages.SetMuteRequest.MuteValueValuesEnum.UNDEFINED
    )

    # Set the finding's Mute State.
    parent = util.ValidateAndGetParent(args)
    if parent is not None:
      util.ValidateSourceAndFindingIdIfParentProvided(args)
      request.name = (
          parent + "/sources/" + args.source + "/findings/" + args.finding
      )
    else:
      request.name = util.GetFindingNameForParent(args)

    # Make the request to the API.
    client = apis.GetClientInstance("securitycenter", "v1")
    response = client.organizations_sources_findings.SetMute(request)
    return response
